from __future__ import annotations

import importlib
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class Dependency:
    package: str
    import_name: str


REQUIRED = [
    Dependency("textual", "textual"),
    Dependency("yfinance", "yfinance"),
    Dependency("baostock", "baostock"),
    Dependency("polars", "polars"),
    Dependency("pyarrow", "pyarrow"),
    Dependency("httpx", "httpx"),
    Dependency("pyyaml", "yaml"),
    Dependency("rich", "rich"),
    Dependency("pandas", "pandas"),
]


def check_deps(auto_install: bool = False) -> list[str]:
    missing: list[str] = []
    for dep in REQUIRED:
        try:
            importlib.import_module(dep.import_name)
        except Exception:
            missing.append(dep.package)
            if auto_install:
                subprocess.run([sys.executable, "-m", "pip", "install", dep.package], check=False)
    return missing
