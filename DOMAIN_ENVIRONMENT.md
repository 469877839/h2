# Reproducible offline environment

Upstream: https://github.com/python-hyper/h2
Source commit: 2e4d22303af8710642f2f493c103860855710f02
Validated on Ubuntu 26.04 / 3.13.15 (main, Sep  1 2026, 14:18:08) [Clang 22.1.3 ].
Environment prepared with Codex; no task implementation is included.

From this checkout with Python 3.13:
```
python -m venv .venv
. .venv/bin/activate
export PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 NO_SQLITE=1 PYYAML_FORCE_LIBYAML=0 SQLGLOT_USE_RUST=0
python -m pip install -r requirements-domain.lock
python -m pip install -e .
python -m pytest -q -o addopts= tests/test_basic_logic.py tests/test_flow_control_window.py tests/test_events.py tests/test_stream_reset.py tests/test_settings.py
```
Only these selected core tests are claimed; the measured process took 1.993 seconds. No external service, account, broker or large downloaded dataset is required. Fixtures retain the upstream license. Restore a clean checkout of this commit and recreate .venv to reset.
