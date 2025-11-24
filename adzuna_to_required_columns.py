import os, time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
COUNTRY = os.getenv("ADZUNA_COUNTRY", "gb")

OUT = "outputs/adzuna_jobs.csv"
QUERY = "data analyst"
PAGES = 20


def fetch_adzuna(page):
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": 50,
        "what": QUERY
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def main():
    all_jobs = []
    for p in range(1, PAGES+1):
        print("Fetching Adzuna page:", p)
        data = fetch_adzuna(p).get("results", [])
        all_jobs.extend(data)
        time.sleep(1)
    df = pd.json_normalize(all_jobs)
    df.to_csv(OUT, index=False)
    print(f"Saved {len(df)} rows → {OUT}")

if __name__ == "__main__":
    main()
