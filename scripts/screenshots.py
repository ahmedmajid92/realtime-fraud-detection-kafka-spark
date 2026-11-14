"""Capture every dashboard page in English and Arabic (run with ``.\\run.ps1 screenshots``).

Runs inside the official Playwright image on the Compose network and writes PNGs to
``docs/screenshots/``. On the Test page it sends a random fraud through Kafka first, so
the screenshot shows a real scored result.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE_URL = os.environ.get("DASHBOARD_URL", "http://dashboard:8501").rstrip("/")
OUT_DIR = Path(os.environ.get("SCREENSHOT_DIR", "/work/out"))
VIEWPORT = {"width": 1600, "height": 2200}
RENDER_WAIT_MS = 6000

PAGES = (
    ("live", "01-live-monitor"),
    ("model-performance", "02-model-performance"),
    ("test-transaction", "03-test-transaction"),
    ("system-health", "04-system-health"),
    ("about", "05-about"),
)
LABELS = {
    "en": {"random_fraud": "Random fraud", "send": "Send via Kafka"},
    "ar": {"random_fraud": "احتيال عشوائي", "send": "إرسال عبر Kafka"},
}


def open_page(page: Page, path: str, lang: str) -> None:
    page.goto(f"{BASE_URL}/{path}", wait_until="domcontentloaded")
    page.wait_for_selector('[data-testid="stSidebar"]', timeout=60_000)
    if lang == "ar":
        page.get_by_text("العربية", exact=True).click()
    page.wait_for_timeout(RENDER_WAIT_MS)


def run_manual_test(page: Page, lang: str) -> None:
    page.get_by_role("button", name=LABELS[lang]["random_fraud"]).click()
    page.wait_for_timeout(2000)
    page.get_by_role("button", name=LABELS[lang]["send"]).click()
    page.wait_for_timeout(8000)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(viewport=VIEWPORT, color_scheme="light")
        page = context.new_page()
        for lang in ("en", "ar"):
            for path, name in PAGES:
                open_page(page, path, lang)
                if path == "test-transaction":
                    run_manual_test(page, lang)
                target = OUT_DIR / f"{name}-{lang}.png"
                page.screenshot(path=str(target), full_page=True)
                print(f"saved {target}")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
