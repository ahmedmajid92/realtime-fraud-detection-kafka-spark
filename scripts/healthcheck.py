"""Container healthcheck helper (stdlib only, so it works in every image).

Usage:
    python scripts/healthcheck.py file <path> <max_age_seconds>   # heartbeat file is fresh
    python scripts/healthcheck.py http <url>                       # URL answers 2xx

Exit code 0 = healthy, 1 = unhealthy.
"""

from __future__ import annotations

import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def check_file(path: str, max_age_s: float) -> bool:
    try:
        age = time.time() - Path(path).stat().st_mtime
    except FileNotFoundError:
        print(f"heartbeat {path} missing")
        return False
    if age > max_age_s:
        print(f"heartbeat {path} is {age:.0f}s old (max {max_age_s:.0f}s)")
        return False
    return True


def check_http(url: str, timeout_s: float = 4.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError) as exc:
        print(f"{url} unreachable: {exc}")
        return False


def main(argv: list[str]) -> int:
    if len(argv) >= 3 and argv[0] == "file":
        return 0 if check_file(argv[1], float(argv[2])) else 1
    if len(argv) >= 2 and argv[0] == "http":
        return 0 if check_http(argv[1]) else 1
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
