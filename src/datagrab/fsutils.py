from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    atomic_write_bytes(path, text.encode(encoding))


def read_text_if_exists(path: Path, encoding: str = "utf-8") -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding=encoding)


def split_csv_line(line: str) -> list[str]:
    return [item.strip() for item in line.split(",")]


def iter_nonempty(values: Iterable[str]) -> list[str]:
    return [v for v in values if v]
