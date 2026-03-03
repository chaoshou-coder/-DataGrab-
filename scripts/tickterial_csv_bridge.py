#!/usr/bin/env python
"""Compatibility wrapper for tickterial CSV to parquet bridge."""

from __future__ import annotations

from datagrab.tickterial.bridge import run


if __name__ == "__main__":
    raise SystemExit(run())
