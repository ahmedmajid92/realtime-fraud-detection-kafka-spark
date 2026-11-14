"""Capture every dashboard page in English and Arabic (run with ``.\\run.ps1 screenshots``).

Runs inside the official Playwright image on the Compose network and writes PNGs to
``docs/screenshots/``. The language is chosen with the dashboard's ``?lang=`` URL
parameter. On the Test page it sends random frauds through Kafka until one is flagged
(up to a few tries), so the screenshot shows a real alert.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE_URL = os.environ.get("DASHBOARD_URL", "http://dashboard:8501").rstrip("/")
OUT_DIR = Path(os.environ.get("SCREENSHOT_DIR", "/work/out"))
VIEWPORT = {"width": 1600, "height": 1500}
RENDER_WAIT_MS = 7000
MAX_FRAUD_TRIES = 4

PAGES = (
    ("", "01-live-monitor"),  # the default page lives at "/"
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
    page.goto(f"{BASE_URL}/{path}?lang={lang}", wait_until="domcontentloaded")
    page.wait_for_selector('[data-testid="stSidebar"]', timeout=60_000)
    page.wait_for_timeout(RENDER_WAIT_MS)


def send_flagged_fraud(page: Page, lang: str) -> None:
    """Send random frauds until the model flags one (most are flagged; some are missed)."""
    for _ in range(MAX_FRAUD_TRIES):
        page.get_by_role("button", name=LABELS[lang]["random_fraud"]).click()
        page.wait_for_timeout(1500)
        page.get_by_role("button", name=LABELS[lang]["send"]).click()
        page.wait_for_timeout(6000)
        if "🚨" in page.locator('[data-testid="stMain"]').inner_text():
            return


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        for lang in ("en", "ar"):
            context = browser.new_context(viewport=VIEWPORT, color_scheme="light")
            page = context.new_page()
            for path, name in PAGES:
                open_page(page, path, lang)
                if path == "test-transaction":
                    send_flagged_fraud(page, lang)
                target = OUT_DIR / f"{name}-{lang}.png"
                page.screenshot(path=str(target), full_page=True)
                print(f"saved {target}")
            context.close()
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
