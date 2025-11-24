import os, time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
JOOBLE_KEY = os.getenv("JOOBLE_API_KEY")

QUERY = "data analyst"
PAGES = 20
OUT = "outputs/jooble_jobs.csv"

def fetch_jooble(page):
    url = f"https://jooble.org/api/{JOOBLE_KEY}"
    payload = {"keywords": QUERY, "page": page}
    r = requests.post(url, json=payload)
    r.raise_for_status()
    return r.json()

def main():
    all_jobs = []
    for p in range(1, PAGES+1):
        print("Fetching Jooble page:", p)
        jobs = fetch_jooble(p).get("jobs", [])
        all_jobs.extend(jobs)
        time.sleep(1)
    df = pd.json_normalize(all_jobs)
    df.to_csv(OUT, index=False)
    print(f"Saved {len(df)} rows → {OUT}")

if __name__ == "__main__":
    main()
