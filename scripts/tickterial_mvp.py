#!/usr/bin/env python
"""Legacy wrapper for tickterial download flow."""

from __future__ import annotations

from datagrab.tickterial.download import parse_args, run


def main() -> None:
    raise SystemExit(run(parse_args()))


if __name__ == "__main__":
    main()
