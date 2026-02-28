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
    Dependency("yfinance", "yfinance"),
    Dependency("baostock", "baostock"),
    Dependency("akshare", "akshare"),
    Dependency("polars", "polars"),
    Dependency("pyarrow", "pyarrow"),
    Dependency("httpx", "httpx"),
    Dependency("pyyaml", "yaml"),
    Dependency("rich", "rich"),
    Dependency("numpy", "numpy"),
    Dependency("pandas", "pandas"),
    Dependency("pydantic", "pydantic"),
]


def check_deps(auto_install: bool = False) -> list[str]:
    missing: list[str] = []
    for dep in REQUIRED:
        try:
            importlib.import_module(dep.import_name)
        except Exception:
            if auto_install:
                subprocess.run([sys.executable, "-m", "pip", "install", dep.package], check=False)
                try:
                    importlib.import_module(dep.import_name)
                except Exception:
                    missing.append(dep.package)
            else:
                missing.append(dep.package)
    return missing
