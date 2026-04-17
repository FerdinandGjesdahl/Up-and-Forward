"""
Up and Forward — Workday Job Scraper
Fetches new finance job listings from Workday-powered career pages.
Run manually or schedule with cron to check for new listings.
"""

import json
import os
import requests
from datetime import datetime

# ── BANKS ON WORKDAY ──────────────────────────────────────────────────────────
# Format: (tenant, workday_instance_number, job_board_name, display_name)
BANKS = [
    # Confirmed working
    ("db",               3, "DBWebsite",                     "Deutsche Bank"),
    ("citi",             5, "2",                             "Citi"),

    # Confirmed Workday — correct board names
    ("ms",               5, "External",                      "Morgan Stanley"),
    ("barclays",         3, "External_Career_Site_Barclays", "Barclays"),
    ("rothschildandco",  3, "Rothschildandco_Lateral",       "Rothschild & Co"),
    ("hl",               1, "External",                      "Houlihan Lokey"),
    ("hl",               1, "Lateral",                       "Houlihan Lokey - Lateral"),
    ("moelis",           1, "Experienced-Hires",             "Moelis & Co"),
    ("moelis",           1, "University-Hires",              "Moelis & Co - University"),
    ("pjtpartners",      1, "Careers",                       "PJT Partners"),
    ("pjtpartners",      1, "Students",                      "PJT Partners - Students"),

    # Goldman, JP Morgan, Lazard, Evercore, Jefferies use other systems
    # — will be added in separate scrapers
]

# ── KEYWORDS TO FILTER FOR ────────────────────────────────────────────────────
KEYWORDS = [
    "intern", "internship",
    "spring week", "spring insight",
    "summer analyst", "summer associate",
    "graduate", "grad programme", "grad program",
    "analyst", "associate",
    "off-cycle", "off cycle",
    "investment banking", "capital markets",
    "IBD", "ECM", "DCM", "M&A",
]

# ── FILES ─────────────────────────────────────────────────────────────────────
SEEN_FILE  = "seen_jobs.json"
OUTPUT_FILE = "new_jobs.json"


def load_seen_jobs():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen_jobs(seen):
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)


def is_relevant(title: str) -> bool:
    title_lower = title.lower()
    return any(kw.lower() in title_lower for kw in KEYWORDS)


def fetch_workday_jobs(tenant: str, instance: int, board: str, bank_name: str) -> list:
    url = (
        f"https://{tenant}.wd{instance}.myworkdayjobs.com"
        f"/wday/cxs/{tenant}/{board}/jobs"
    )
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    }
    payload = {
        "limit": 20,
        "offset": 0,
        "searchText": "",
        "appliedFacets": {}
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("jobPostings", [])

        results = []
        for job in jobs:
            title    = job.get("title", "")
            job_id   = job.get("externalPath", job.get("bulletFields", [title])[0])
            location = ", ".join(job.get("locationsText", "").split(",")[:2])
            url_path = job.get("externalPath", "")
            apply_url = f"https://{tenant}.wd{instance}.myworkdayjobs.com/en-US/{board}{url_path}"

            if is_relevant(title):
                results.append({
                    "id":       f"{tenant}_{job_id}",
                    "bank":     bank_name,
                    "title":    title,
                    "location": location,
                    "url":      apply_url,
                    "found_at": datetime.now().isoformat(),
                })
        return results

    except requests.exceptions.RequestException as e:
        print(f"  ✗ {bank_name}: {e}")
        return []


def run():
    print(f"\n{'='*55}")
    print(f"  Up and Forward — Workday Scraper")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    seen     = load_seen_jobs()
    new_jobs = []

    for tenant, instance, board, bank_name in BANKS:
        print(f"Checking {bank_name}...")
        jobs = fetch_workday_jobs(tenant, instance, board, bank_name)

        for job in jobs:
            if job["id"] not in seen:
                new_jobs.append(job)
                seen.add(job["id"])
                print(f"  ✓ NEW: {job['title']} — {job['location']}")

        if not jobs:
            print(f"  → No relevant listings found")

    save_seen_jobs(seen)

    if new_jobs:
        with open(OUTPUT_FILE, "w") as f:
            json.dump(new_jobs, f, indent=2)
        print(f"\n{'='*55}")
        print(f"  {len(new_jobs)} new listing(s) saved to {OUTPUT_FILE}")
        print(f"{'='*55}\n")
    else:
        print(f"\n  No new listings since last run.\n")

    return new_jobs


if __name__ == "__main__":
    run()
