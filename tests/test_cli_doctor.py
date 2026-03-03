import sys
from argparse import Namespace
from types import ModuleType, SimpleNamespace
import json
from io import StringIO


from datagrab.cli import _run_doctor, build_parser
from datagrab.logging import get_logger


def test_doctor_parser_supports_required_options():
    parser = build_parser()
    args = parser.parse_args(
        [
            "doctor",
            "--json",
            "--strict",
            "--check-scope",
            "--symbol",
            "AAPL",
            "--interval",
            "1d",
        ]
    )
    assert args.command == "doctor"
    assert args.json is True
    assert args.strict is True
    assert args.check_scope is True
    assert args.symbol == ["AAPL"]
    assert args.interval == "1d"


def test_run_doctor_smoke_returns_ok(monkeypatch, tmp_path):
    fake_httpx = ModuleType("httpx")

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, follow_redirects=True):
            return FakeResponse()

    fake_httpx.Client = FakeClient
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

    monkeypatch.setattr("datagrab.cli.check_deps", lambda auto_install=False: [])

    args = Namespace(
        command="doctor",
        json=False,
        strict=False,
        check_scope=False,
        asset_type="stock",
        symbols=None,
        symbol=None,
        interval=None,
    )
    config = SimpleNamespace(
        timezone="Asia/Shanghai",
        asset_types=["stock", "ashare", "forex", "crypto", "commodity"],
        storage=SimpleNamespace(data_root=str(tmp_path), merge_on_incremental=True),
        rate_limit=SimpleNamespace(requests_per_second=1.0, jitter_min=0.2, jitter_max=0.6),
        yfinance=SimpleNamespace(proxy=None),
        data_root_path=tmp_path,
    )
    logger = get_logger("datagrab.cli-doctor-smoke")

    code = _run_doctor(args, config, logger)
    assert code == 0


def test_run_doctor_optional_screener_fail_keeps_strict_non_blocking(monkeypatch, tmp_path):
    output = StringIO()
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: output.write(" ".join(str(item) for item in args) + "\n"))

    fake_httpx = ModuleType("httpx")

    class FakeResponse:
        def __init__(self, status_code: int):
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"status={self.status_code}")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, url, follow_redirects=True):
            if "screener" in url:
                return FakeResponse(404)
            return FakeResponse(200)

    fake_httpx.Client = FakeClient
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

    monkeypatch.setattr("datagrab.cli.check_deps", lambda auto_install=False: [])

    args = Namespace(
        command="doctor",
        json=True,
        strict=True,
        check_scope=False,
        asset_type="stock",
        symbols=None,
        symbol=None,
        interval=None,
    )
    config = SimpleNamespace(
        timezone="Asia/Shanghai",
        asset_types=["stock", "ashare", "forex", "crypto", "commodity"],
        storage=SimpleNamespace(data_root=str(tmp_path), merge_on_incremental=True),
        rate_limit=SimpleNamespace(requests_per_second=1.0, jitter_min=0.2, jitter_max=0.6),
        yfinance=SimpleNamespace(proxy=None),
        data_root_path=tmp_path,
    )
    logger = get_logger("datagrab.cli-doctor-screener-soft")

    code = _run_doctor(args, config, logger)
    assert code == 0

    payload = output.getvalue().strip().splitlines()
    assert payload, "should print json report"
    report = json.loads("".join(payload))
    assert report["checks"]["network"]["status"] == "warn"
