import os
from pathlib import Path


def _default_csv_path() -> Path:
    """Return the built-in fallback CSV path (relative to the repo)."""
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "bigdata" / "latest.csv"


def _resolve_csv_path() -> Path:
    """Resolve the CSV path from env vars, falling back to the default location."""
    env_value = os.environ.get("COMPFILTER_CSV") or os.environ.get("CSV_PATH")
    if env_value:
        return Path(env_value).expanduser()
    return _default_csv_path()


CSV_PATH = _resolve_csv_path()
CSV_DELIMITER = ';'
CSV_ENCODING = 'utf-8'
HOST = "127.0.0.1"
PORT = int(os.environ.get("PORT", "3004"))
DEBUG = True
