# jsearch_api_to_csv.py
import requests
import time
import json
import pandas as pd
from pandas import json_normalize

# CONFIG
API_URL = "https://jsearch.p.rapidapi.com/search"
API_KEY = "4473354741msh1a226b5b22fe3d7p1270b2jsnec7a193cc326"  # move to env/secret in production
API_HOST = "jsearch.p.rapidapi.com"
QUERY = "data analyst jobs in uk"
START_PAGE = 1
NUM_PAGES = 20   # increase if you want more pages
OUTPUT_CSV = "jsearch_jobs.csv"
OUTPUT_XLSX = "jsearch_jobs.xlsx"
SLEEP_BETWEEN_REQUESTS = 1  # seconds to avoid rate limits

headers = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST,
    "Accept": "application/json"
}

def fetch_page(page):
    params = {
        "query": QUERY,
        "page": str(page),
        "num_pages": "1"  # we fetch one page at a time
    }
    resp = requests.get(API_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()

def normalize_jobs(job_list):
    """
    job_list: list of job dicts from API 'data' field
    Returns a pandas DataFrame with flattened columns.
    """
    # Use json_normalize to flatten nested structures. 
    df = json_normalize(job_list)
    
    # Optional: pick / reorder columns you care about, keep full description
    desired_cols = [
        'job_id', 'job_title', 'employer_name', 'employer_website', 'job_publisher',
        'job_employment_type', 'job_apply_link', 'job_description',
        'job_is_remote', 'job_posted_at', 'job_posted_at_datetime_utc',
        'job_location', 'job_city', 'job_country', 'job_latitude', 'job_longitude',
        'job_salary', 'job_min_salary', 'job_max_salary', 'job_salary_period',
        'job_onet_soc', 'job_onet_job_zone'
    ]
    # Keep only columns that exist in the df
    cols_existing = [c for c in desired_cols if c in df.columns]
    df = df[cols_existing]
    
    # Clean / normalize some columns if needed
    if 'job_posted_at_timestamp' in df.columns:
        df['job_posted_at_ts'] = pd.to_datetime(df['job_posted_at_timestamp'], unit='s', utc=True)
    return df

def main():
    all_rows = []
    page = START_PAGE
    pages_fetched = 0

    while pages_fetched < NUM_PAGES:
        print(f"Fetching page {page} ...")
        try:
            resp_json = fetch_page(page)
        except Exception as e:
            print("Error fetching page:", e)
            break

        data = resp_json.get("data") or []
        if not data:
            print("No data returned on page", page)
            break

        # Append raw job dicts to list
        all_rows.extend(data)

        pages_fetched += 1
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    if not all_rows:
        print("No jobs fetched. Exiting.")
        return

    # Normalize into dataframe
    df = normalize_jobs(all_rows)

    # OPTIONAL: Expand apply_options (first apply link)
    if 'apply_options' in json.dumps(all_rows):
        # json_normalize with record_path can expand nested arrays; we'll extract first apply link per job
        def get_first_apply_link(job):
            ao = job.get('apply_options') or []
            if isinstance(ao, list) and len(ao) > 0:
                return ao[0].get('apply_link') or ao[0].get('publisher') or None
            return None
        df['first_apply_link'] = [get_first_apply_link(j) for j in all_rows]

    # Save CSV (UTF-8)
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    print(f"Saved {len(df)} rows to {OUTPUT_CSV}")

    # Save Excel
    # If description is very long, Excel will still store it; use openpyxl engine
    df.to_excel(OUTPUT_XLSX, index=False, engine='openpyxl')
    print(f"Saved {len(df)} rows to {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
