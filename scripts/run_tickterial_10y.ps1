param(
    [ValidateSet("download", "resume", "bridge", "validate", "all")]
    [string]$Action = "download",

    [string]$ProjectRoot = "E:\code\datagrab",
    [string]$OutputDir = "E:\stock_data\DateGrab\tickterial_csv",
    [string]$CacheDir = ".tick-data",
    [string]$Symbols = "XAUUSD,XAGUSD",
    [string]$Intervals = "1m,5m,15m,1d",

    [int]$StartYear = 2016,
    [int]$EndYearExclusive = 2026,
    [string]$ValidationWindowStart = "2025-01-03T00:00:00",
    [string]$ValidationWindowEnd = "2025-01-04T00:00:00",

    [int]$MaxRetries = 6,
    [double]$RetryDelay = 2.0,
    [int]$DownloadWorkers = 0,
    [int]$BatchSize = 0,
    [int]$BatchPauseMs = 0,
    [int]$RetryJitterMs = 0,
    [string]$LogLevel = "INFO",
    [switch]$SuppressTickloaderInfo = $false,
    [double]$SourceTimestampShiftHours = 8.0,
    [ValidateSet("safe", "balanced", "aggressive")]
    [string]$ConcurrencyPreset = "balanced",
    [int]$WindowRetries = 1,
    [bool]$StrictValidate = $true,
    [bool]$Validate = $true,
    [bool]$Force = $false,

    [string]$ParquetRoot = "E:\code\datagrab\data",
    [bool]$MergeOnIncremental = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-Timestamp {
    return Get-Date -Format "yyyyMMdd_HHmmss"
}

function Ensure-Directory {
    param([Parameter(Mandatory = $true)][string]$PathValue)
    if (-not (Test-Path -LiteralPath $PathValue)) {
        New-Item -ItemType Directory -Path $PathValue -Force | Out-Null
    }
}

function Resolve-ConcurrencyProfile {
    param(
        [Parameter(Mandatory = $true)][string]$Preset
    )
    switch ($Preset) {
        "safe" {
            return [pscustomobject]@{
                DownloadWorkers = 2
                BatchSize = 4
                BatchPauseMs = 1800
                RetryJitterMs = 300
            }
        }
        "aggressive" {
            return [pscustomobject]@{
                DownloadWorkers = 8
                BatchSize = 12
                BatchPauseMs = 500
                RetryJitterMs = 300
            }
        }
        default {
            return [pscustomobject]@{
                DownloadWorkers = 4
                BatchSize = 8
                BatchPauseMs = 1000
                RetryJitterMs = 300
            }
        }
    }
}

function Invoke-PythonCommand {
    param(
        [Parameter(Mandatory = $true)][string[]]$PythonArgs,
        [Parameter(Mandatory = $true)][string]$LogPath
    )
    $oldEap = $ErrorActionPreference
    $hasNativePreference = $false
    $oldNativePreference = $null
    if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
        $hasNativePreference = $true
        $oldNativePreference = $PSNativeCommandUseErrorActionPreference
    }
    try {
        # tickterial logs to stderr; treat it as normal command output.
        $ErrorActionPreference = "Continue"
        if ($hasNativePreference) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        & python @PythonArgs 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $LogPath
        return $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $oldEap
        if ($hasNativePreference) {
            $PSNativeCommandUseErrorActionPreference = $oldNativePreference
        }
    }
}

function Invoke-MvpDownload {
    param(
        [Parameter(Mandatory = $true)][string]$StartIso,
        [Parameter(Mandatory = $true)][string]$EndIso,
        [bool]$ResumeOnly = $false,
        [string]$OutputDirOverride = "",
        [int]$DownloadWorkersOverride = 0,
        [int]$BatchSizeOverride = 0,
        [int]$BatchPauseMsOverride = 0,
        [int]$RetryJitterMsOverride = 0
    )
    $profile = Resolve-ConcurrencyProfile -Preset $ConcurrencyPreset
    $resolvedWorkers = if ($DownloadWorkersOverride -gt 0) {
        $DownloadWorkersOverride
    } elseif ($DownloadWorkers -gt 0) {
        $DownloadWorkers
    } else {
        $profile.DownloadWorkers
    }
    $resolvedBatchSize = if ($BatchSizeOverride -gt 0) {
        $BatchSizeOverride
    } elseif ($BatchSize -gt 0) {
        $BatchSize
    } else {
        $profile.BatchSize
    }
    $resolvedBatchPauseMs = if ($BatchPauseMsOverride -gt 0) {
        $BatchPauseMsOverride
    } elseif ($BatchPauseMs -gt 0) {
        $BatchPauseMs
    } else {
        $profile.BatchPauseMs
    }
    $resolvedRetryJitterMs = if ($RetryJitterMsOverride -gt 0) {
        $RetryJitterMsOverride
    } elseif ($RetryJitterMs -gt 0) {
        $RetryJitterMs
    } else {
        $profile.RetryJitterMs
    }

    $pythonArgs = @(
        "scripts/tickterial_mvp.py",
        "--start", $StartIso,
        "--end", $EndIso,
        "--symbols", $Symbols,
        "--output", $(if ($OutputDirOverride) { $OutputDirOverride } else { $OutputDir }),
        "--cache-dir", $CacheDir,
        "--intervals", $Intervals,
        "--max-retries", $MaxRetries.ToString(),
        "--retry-delay", ([Convert]::ToString($RetryDelay, [System.Globalization.CultureInfo]::InvariantCulture)),
        "--download-workers", $resolvedWorkers.ToString(),
        "--batch-size", $resolvedBatchSize.ToString(),
        "--batch-pause-ms", $resolvedBatchPauseMs.ToString(),
        "--retry-jitter-ms", $resolvedRetryJitterMs.ToString(),
        "--source-timestamp-shift-hours", ([Convert]::ToString($SourceTimestampShiftHours, [System.Globalization.CultureInfo]::InvariantCulture)),
        "--log-level", $LogLevel,
        "--window-retries", $WindowRetries.ToString()
    )

    if ($StrictValidate) {
        $pythonArgs += "--strict-validate"
    }
    else {
        $pythonArgs += "--no-strict-validate"
    }

    if ($Validate) {
        $pythonArgs += "--validate"
    }
    if ($Force) {
        $pythonArgs += "--force"
    }
    if ($SuppressTickloaderInfo) {
        $pythonArgs += "--suppress-tickloader-info"
    }

    $failureCsv = Join-Path $(if ($OutputDirOverride) { $OutputDirOverride } else { $OutputDir }) "failures_mvp.csv"
    if ($ResumeOnly) {
        if (-not (Test-Path -LiteralPath $failureCsv)) {
            Write-Host "No failures_mvp.csv found, skip resume."
            return 0
        }
        $pythonArgs += @("--resume-failures", $failureCsv)
    }

    $tag = "{0}_{1}" -f ($StartIso.Substring(0, 10).Replace("-", "")), ($EndIso.Substring(0, 10).Replace("-", ""))
    $logName = if ($ResumeOnly) { "resume_${tag}_$(Get-Timestamp).log" } else { "run_${tag}_$(Get-Timestamp).log" }
    $logPath = Join-Path $(if ($OutputDirOverride) { $OutputDirOverride } else { $OutputDir }) $logName

    Write-Host "Resolved concurrency: workers=$resolvedWorkers batchSize=$resolvedBatchSize batchPauseMs=$resolvedBatchPauseMs retryJitterMs=$resolvedRetryJitterMs"
    Write-Host "Run: python $($pythonArgs -join ' ')"
    $code = Invoke-PythonCommand -PythonArgs $pythonArgs -LogPath $logPath
    Write-Host "Log saved: $logPath"
    return $code
}

function Invoke-Bridge {
    $pythonArgs = @(
        "scripts/tickterial_csv_bridge.py",
        "--input-dir", $OutputDir,
        "--output-root", $ParquetRoot,
        "--asset-type", "commodity"
    )
    if ($MergeOnIncremental) {
        $pythonArgs += "--merge-on-incremental"
    }
    $logPath = Join-Path $OutputDir ("bridge_{0}.log" -f (Get-Timestamp))
    Write-Host "Run: python $($pythonArgs -join ' ')"
    $code = Invoke-PythonCommand -PythonArgs $pythonArgs -LogPath $logPath
    Write-Host "Log saved: $logPath"
    return $code
}

function Invoke-ThroughputValidation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$StartIso,
        [Parameter(Mandatory = $true)][string]$EndIso
    )

    $windowTag = "{0}_{1}" -f ($StartIso.Substring(0, 10).Replace("-", "")), ($EndIso.Substring(0, 10).Replace("-", ""))
    $serialDir = Join-Path $OutputDir ("throughput_{0}_serial" -f $windowTag)
    $parallelDir = Join-Path $OutputDir ("throughput_{0}_parallel" -f $windowTag)
    Ensure-Directory -PathValue $serialDir
    Ensure-Directory -PathValue $parallelDir

    Write-Host "=== Throughput validation: serial baseline ==="
    $serialTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $serialCode = Invoke-MvpDownload -StartIso $StartIso -EndIso $EndIso -OutputDirOverride $serialDir -DownloadWorkersOverride 1 -BatchSizeOverride 1 -BatchPauseMsOverride 2000
    $serialTimer.Stop()
    if ($serialCode -ne 0) {
        throw "Serial validation run failed: $serialCode"
    }

    Write-Host "=== Throughput validation: balanced run ==="
    $parallelTimer = [System.Diagnostics.Stopwatch]::StartNew()
    $parallelCode = Invoke-MvpDownload -StartIso $StartIso -EndIso $EndIso -OutputDirOverride $parallelDir
    $parallelTimer.Stop()
    if ($parallelCode -ne 0) {
        throw "Parallel validation run failed: $parallelCode"
    }

    $symbols = $Symbols -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $intervals = $Intervals -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    foreach ($symbol in $symbols) {
        foreach ($interval in $intervals) {
            $serialFile = Join-Path $serialDir ("{0}_{1}_{2}.csv" -f $symbol, $interval, $windowTag)
            $parallelFile = Join-Path $parallelDir ("{0}_{1}_{2}.csv" -f $symbol, $interval, $windowTag)
            if (-not (Test-Path -LiteralPath $serialFile)) {
                Write-Warning "Serial file missing: $serialFile"
                continue
            }
            if (-not (Test-Path -LiteralPath $parallelFile)) {
                Write-Warning "Parallel file missing: $parallelFile"
                continue
            }
            $serialCount = (Import-Csv $serialFile).Count
            $parallelCount = (Import-Csv $parallelFile).Count
            if ($serialCount -ne $parallelCount) {
                Write-Warning ("Row count mismatch for {0}_{1}: serial={2}, parallel={3}" -f $symbol, $interval, $serialCount, $parallelCount)
            } else {
                Write-Host ("Consistency ok for {0}_{1}: {2} rows" -f $symbol, $interval, $serialCount)
            }
        }
    }

    $ratio = if ($parallelTimer.Elapsed.TotalSeconds -gt 0) {
        [Math]::Round($serialTimer.Elapsed.TotalSeconds / $parallelTimer.Elapsed.TotalSeconds, 2)
    } else {
        0
    }
    Write-Host ("Serial: {0}s" -f [Math]::Round($serialTimer.Elapsed.TotalSeconds, 2))
    Write-Host ("Parallel: {0}s" -f [Math]::Round($parallelTimer.Elapsed.TotalSeconds, 2))
    Write-Host ("Throughput improvement x: {0}" -f $ratio)
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python not found in PATH. Activate venv first."
}

if (-not (Test-Path -LiteralPath (Join-Path $ProjectRoot "scripts/tickterial_mvp.py"))) {
    throw "scripts/tickterial_mvp.py not found in project root: $ProjectRoot"
}
if (-not (Test-Path -LiteralPath (Join-Path $ProjectRoot "scripts/tickterial_csv_bridge.py"))) {
    throw "scripts/tickterial_csv_bridge.py not found in project root: $ProjectRoot"
}

Ensure-Directory -PathValue $OutputDir

Push-Location $ProjectRoot
try {
    if ($Action -eq "download" -or $Action -eq "all") {
        $failedYears = @()
        for ($y = $StartYear; $y -lt $EndYearExclusive; $y++) {
            $startIso = "{0}-01-01T00:00:00" -f $y
            $endIso = "{0}-01-01T00:00:00" -f ($y + 1)
            Write-Host "=== Year $y ==="
            $code = Invoke-MvpDownload -StartIso $startIso -EndIso $endIso
            if ($code -ne 0) {
                $failedYears += $y
                Write-Warning "Year $y finished with exit code $code"
            }
            Start-Sleep -Seconds 2
        }
        if ($failedYears.Count -gt 0) {
            Write-Warning ("Failed years: {0}" -f ($failedYears -join ", "))
        } else {
            Write-Host "Download stage completed with no failed years."
        }
    }

    if ($Action -eq "resume" -or $Action -eq "all") {
        $startIso = "{0}-01-01T00:00:00" -f $StartYear
        $endIso = "{0}-01-01T00:00:00" -f $EndYearExclusive
        Write-Host "=== Resume failures ==="
        $resumeCode = Invoke-MvpDownload -StartIso $startIso -EndIso $endIso -ResumeOnly $true
        if ($resumeCode -ne 0) {
            Write-Warning "Resume stage exit code: $resumeCode"
        } else {
            Write-Host "Resume stage completed."
        }
    }

    if ($Action -eq "bridge" -or $Action -eq "all") {
        Write-Host "=== CSV to Parquet bridge ==="
        $bridgeCode = Invoke-Bridge
        if ($bridgeCode -ne 0) {
            throw "Bridge stage failed with exit code $bridgeCode"
        }
        Write-Host "Bridge stage completed."
    }

    if ($Action -eq "validate") {
        Invoke-ThroughputValidation -StartIso $ValidationWindowStart -EndIso $ValidationWindowEnd
    }
}
finally {
    Pop-Location
}

Write-Host "Done."
