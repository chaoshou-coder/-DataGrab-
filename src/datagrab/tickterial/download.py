"""Backward-compatible re-export layer.

After the Phase 2 split, the actual logic lives in:

- :mod:`~datagrab.tickterial.fetch` — concurrent tick downloading
- :mod:`~datagrab.tickterial.aggregate` — OHLCV bar building and validation
- :mod:`~datagrab.tickterial.runner` — CLI orchestration, CSV I/O, failure tracking

All public names are re-exported here so that existing callers
(``from datagrab.tickterial import download as tickterial_download``)
continue to work without changes.
"""

from __future__ import annotations

# Re-export from fetch
from .fetch import (  # noqa: F401
    DEFAULT_ADAPTIVE_FAIL_RATE,
    HourTaskResult,
    Tickloader,
    extract_price,
    extract_volume,
    fetch_ticks,
    to_float,
)

# Re-export from aggregate
from .aggregate import (  # noqa: F401
    build_1m_bars,
    build_daily_bars_ny_close,
    build_multi_interval_bars,
    check_interval_integrity,
    check_ny_close_alignment,
    check_ohlc_consistency,
)

# Re-export from runner
from .runner import (  # noqa: F401
    FailedWindow,
    append_failure,
    configure_logging,
    iter_year_windows,
    parse_args,
    read_failed_windows,
    run,
    write_csv,
)

# Re-export common utilities used by external callers
from .common import (  # noqa: F401
    parse_dt,
    parse_intervals,
    parse_symbols,
    to_minute_floor as floor_to_minute,
)
