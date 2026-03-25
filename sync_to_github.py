"""
sync_to_github.py
-----------------
Run this after scraper.py to push dashboard_data.json to GitHub.
Vercel detects the push and republishes your dashboard automatically.

Setup (one time):
    1. pip3 install requests
    2. Create a GitHub Personal Access Token:
       - Go to github.com → Settings → Developer settings
       - Personal access tokens → Tokens (classic) → Generate new token
       - Give it a name like "rave-tracker"
       - Check the "repo" scope
       - Copy the token and paste it below as GITHUB_TOKEN
    3. Fill in GITHUB_REPO and GITHUB_USERNAME below
    4. Run: python3 sync_to_github.py
"""

import base64
import json
import os
from pathlib import Path
import requests

# ─── Fill these in ────────────────────────────────────────────────────────────
GITHUB_TOKEN    = "YOUR_GITHUB_TOKEN_HERE"
GITHUB_USERNAME = "YOUR_GITHUB_USERNAME_HERE"
GITHUB_REPO     = "rave-tracker"           # the repo name you'll create
FILE_PATH       = "dashboard_data.json"    # path inside the repo
# ─────────────────────────────────────────────────────────────────────────────

LOCAL_FILE = Path(__file__).parent.parent / "crowdvolt_scraper" / "dashboard_data.json"

def push_to_github():
    if not LOCAL_FILE.exists():
        print(f"ERROR: {LOCAL_FILE} not found. Run scraper.py first.")
        return

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{FILE_PATH}"

    # Get current SHA if file exists (needed for updates)
    sha = None
    r = requests.get(api_url, headers=headers)
    if r.status_code == 200:
        sha = r.json()["sha"]

    # Encode file content
    content = LOCAL_FILE.read_bytes()
    encoded = base64.b64encode(content).decode()

    payload = {
        "message": "Auto-update prices from scraper",
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(api_url, headers=headers, json=payload)
    if r.status_code in (200, 201):
        print(f"✓ Pushed {FILE_PATH} to GitHub → Vercel will update in ~10 seconds")
    else:
        print(f"ERROR pushing to GitHub: {r.status_code} {r.text}")

if __name__ == "__main__":
    push_to_github()
