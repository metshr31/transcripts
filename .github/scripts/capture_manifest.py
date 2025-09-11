name: Playwright (Python) — Headless & Xvfb

on:
  push:
  workflow_dispatch:

jobs:
  playwright-headless:
    name: A) Headless Chromium (no X11)
    runs-on: ubuntu-latest
    env:
      PIP_DISABLE_PIP_VERSION_CHECK: "1"
      PYTHONUNBUFFERED: "1"
      # If your code respects this, it will force headless, but also set headless=True in code.
      PW_HEADLESS: "1"
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Python deps
        run: |
          python -m pip install --upgrade pip
          pip install playwright
          # add your other deps here, e.g.:
          # pip install -r requirements.txt

      - name: Install Playwright browsers
        run: |
          python -m playwright install chromium
          # Pull in OS libraries Playwright needs (avoids Missing X/$DISPLAY errors in headless):
          sudo python -m playwright install-deps

      - name: Run your script (headless)
        run: |
          python scripts/capture_manifest.py
        # If you need extra Chromium flags at launch, add them in your Python code:
        # browser = await playwright.chromium.launch(headless=True, args=["--no-sandbox"])

  playwright-xvfb:
    name: B) Chromium under Xvfb (virtual display)
    runs-on: ubuntu-latest
    env:
      PIP_DISABLE_PIP_VERSION_CHECK: "1"
      PYTHONUNBUFFERED: "1"
      # Headed-style runs benefit from XVFB; your code can set headless=False for recordings/screenshots.
      PW_HEADLESS: "0"
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Python deps
        run: |
          python -m pip install --upgrade pip
          pip install playwright
          # add your other deps here, e.g.:
          # pip install -r requirements.txt

      - name: Install Playwright browsers + OS libs
        run: |
          python -m playwright install chromium
          sudo python -m playwright install-deps
          # (Optional) If you record video/audio, you may also want ffmpeg:
          # sudo apt-get update && sudo apt-get install -y ffmpeg

      - name: Run your script under Xvfb
        run: |
          xvfb-run -a python scripts/capture_manifest.py
        # In your Python code, launch with headless=False for a true headed session:
        # browser = await playwright.chromium.launch(headless=False, args=["--no-sandbox"])