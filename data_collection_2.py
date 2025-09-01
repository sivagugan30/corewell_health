import requests, datetime, time, pandas as pd

BASE_URL = "https://api.nsf.gov/services/v1/awards.json"
HEADERS = {"User-Agent": "nsf-fetcher/1.0 (+contact@example.com)"}  # be nice

# --- Search settings ---
ORG_CANDIDATES = [
    "Corewell Health",
    "Spectrum Health",
    "Beaumont Health",
    "William Beaumont Hospital",
    "Helen DeVos Children's Hospital",
    "Butterworth Hospital",
    "Lakeland Hospital",
    "Blodgett Hospital",
]
YEARS_BACK = 20  # widen the window for legacy names

# --- Date window ---
today = datetime.date.today()
start_date = (today - datetime.timedelta(days=365 * YEARS_BACK)).strftime("%m/%d/%Y")
end_date = today.strftime("%m/%d/%Y")

# All valid printable fields you care about (from docs)
PRINT_FIELDS = ",".join([
    "id","agency","awardeeName","awardeeCity","awardeeStateCode",
    "piFirstName","piLastName","title","date","startDate","expDate",
    "fundsObligatedAmt","abstractText"
])

def fetch_page(params):
    """Call API once; return (awards, service_notifications)."""
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    resp = data.get("response", {})
    notes = resp.get("serviceNotification", [])
    awards = resp.get("award", [])
    return awards, notes

def fetch_all(params, max_pages=200, pause=0.25):
    """Paginate using rpp/offset per docs."""
    out = []
    offset = 1
    pages = 0
    while True:
        p = dict(params, rpp=25, offset=offset, printFields=PRINT_FIELDS)
        awards, notes = fetch_page(p)

        # If API returned a serviceNotification error, stop gracefully
        if notes:
            msg = "; ".join(n.get("notificationMessage","") for n in notes)
            code = ", ".join(n.get("notificationCode","") for n in notes)
            raise RuntimeError(f"NSF API error [{code}]: {msg}")

        if not awards:
            break

        out.extend(awards)
        pages += 1
        if len(awards) < 25 or pages >= max_pages:
            break
        offset += 25
        time.sleep(pause)  # be polite
    return out

def search_orgs():
    all_rows = []

    # 1) Exact org matches via awardeeName (best signal)
    for org in ORG_CANDIDATES:
        all_rows += fetch_all({
            "awardeeName": org,
            "dateStart": start_date,     # “Award Date (Initial Amendment)” window
            "dateEnd": end_date
        })

    # 2) Fuzzy fallback via keyword (phrase and unquoted)
    for org in ORG_CANDIDATES:
        for term in (f"\"{org}\"", org):
            all_rows += fetch_all({
                "keyword": term,
                "dateStart": start_date,
                "dateEnd": end_date
            })

    # 3) Safety net: Michigan-only sweep for org text variants
    all_rows += fetch_all({
        "awardeeStateCode": "MI",
        "keyword": "Corewell OR Spectrum OR Beaumont",
        "dateStart": start_date,
        "dateEnd": end_date
    })

    # Optional: try startDate window instead of award date window
    if not all_rows:
        for org in ORG_CANDIDATES:
            all_rows += fetch_all({
                "awardeeName": org,
                "startDateStart": start_date,  # “Award Start Date” window
                "startDateEnd": end_date
            })

    return all_rows

def to_df(rows):
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates(subset=["id"])
    # normalize money as numeric if present
    if "fundsObligatedAmt" in df.columns:
        df["fundsObligatedAmt"] = pd.to_numeric(df["fundsObligatedAmt"], errors="coerce")
    # convenient ordering if fields exist
    cols = [c for c in [
        "id","title","awardeeName","awardeeCity","awardeeStateCode",
        "piFirstName","piLastName","date","startDate","expDate","fundsObligatedAmt"
    ] if c in df.columns]
    return df[cols] if cols else df

if __name__ == "__main__":
    try:
        rows = search_orgs()
        df = to_df(rows)
        print(f"Total NSF awards found: {len(df)}")
        if df.empty:
            print("No NSF awards matched Corewell/Spectrum/Beaumont in the last 20 years.")
        else:
            print(df.head(25).to_string(index=False))
    except requests.HTTPError as e:
        print(f"HTTP error: {e}")   # e.g., maintenance window
    except RuntimeError as e:
        print(f"API reported an error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")