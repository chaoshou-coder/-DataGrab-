from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable


class Severity(str, Enum):
    WARN = "WARN"
    ERROR = "ERROR"


@dataclass(frozen=True)
class QualityIssue:
    """数据质量问题（不等同于下载失败）。"""

    rule_id: str
    severity: Severity
    message: str
    path: str | None = None
    asset_type: str | None = None
    symbol: str | None = None
    interval: str | None = None
    details: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Enum → str
        d["severity"] = self.severity.value
        return d


@dataclass
class QualityReport:
    issues: list[QualityIssue] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.ERROR)

    @property
    def warn_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.WARN)

    def extend(self, issues: Iterable[QualityIssue]) -> None:
        self.issues.extend(list(issues))


def write_issues_jsonl(path: Path, issues: Iterable[QualityIssue]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for issue in issues:
            f.write(json.dumps(issue.to_dict(), ensure_ascii=False))
            f.write("\n")


def write_issues_csv(path: Path, issues: Iterable[QualityIssue]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [i.to_dict() for i in issues]
    # 固定列顺序，便于 Excel 查看
    fieldnames = [
        "created_at",
        "severity",
        "rule_id",
        "asset_type",
        "symbol",
        "interval",
        "path",
        "message",
        "details",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

