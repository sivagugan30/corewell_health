import os, time, datetime, requests, pandas as pd
from requests.adapters import HTTPAdapter
try:
    # Retry is vendored under urllib3 in requests
    from urllib3.util.retry import Retry
except Exception:
    Retry = None  # Fallback if environment lacks urllib3 Retry

BASE_URL = "https://api.nsf.gov/services/v1/awards.json"
HEADERS = {"User-Agent": "nsf-fetcher/1.0 (+contact@example.com)"}  # be nice

# --- Search settings ---
ORG_CANDIDATES = [
    "Corewell Health",
    "Spectrum Health",
    "Beaumont Health",
]
YEARS_BACK = 25  # widen the window for legacy names

# --- Date window ---
today = datetime.date.today()
start_date = (today - datetime.timedelta(days=365 * YEARS_BACK)).strftime("%m/%d/%Y")
end_date = today.strftime("%m/%d/%Y")

# All valid printable fields you care about (from docs)
PRINT_FIELDS = ",".join([
    "id","agency","awardeeCity","awardeeCountryCode","awardeeDistrictCode",
    "awardeeName","awardeeStateCode","awardeeZipCode","awdSpAttnCode",
    "awdSpAttnDesc","cfdaNumber","coPDPI","date","startDate","expDate",
    "estimatedTotalAmt","fundsObligatedAmt","ueiNumber","fundProgramName",
    "parentUeiNumber","pdPIName","perfCity","perfCountryCode",
    "perfDistrictCode","perfLocation","perfStateCode","perfZipCode",
    "poName","primaryProgram","transType","title","awardee",
    "poPhone","poEmail","awardeeAddress","perfAddress",
    "publicationResearch","publicationConference","fundAgencyCode",
    "awardAgencyCode","projectOutComesReport","abstractText",
    "piFirstName","piMiddeInitial","piLastName","piEmail"
])

# ---------------------------
# HTTP session with retries
# ---------------------------
def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    if Retry:
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"])
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
    return s

SESSION = make_session()

# ---------------------------
# Fetching helpers
# ---------------------------
def fetch_page(params):
    """Call API once; return (awards, service_notifications)."""
    r = SESSION.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    resp = data.get("response", {})
    notes = resp.get("serviceNotification", []) or []
    awards = resp.get("award", []) or []
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

# ---------------------------
# Org matching logic (strict)
# ---------------------------
def _norm(s):
    return (s or "").lower().strip()

ORG_TOKENS = [o.lower() for o in ORG_CANDIDATES]

def fetch_nsf_awards(org_names, start_date, end_date):
    base = "https://api.nsf.gov/services/v1/awards.json"
    s = start_date.strftime("%m/%d/%Y")
    e = end_date.strftime("%m/%d/%Y")
    rpp = 25

    rows = []
    for org in org_names:
        offset = 1
        while True:
            params = {
                "awardeeName": org,
                "startDateStart": s,
                "startDateEnd": e,
                "rpp": rpp,
                "offset": offset,
                "printFields": ",".join([
                    "id","agency","awardeeName","awardeeCity","awardeeStateCode",
                    "pdPIName","piFirstName","piLastName","title","date","startDate","expDate",
                    "fundsObligatedAmt","abstractText"
                ])
            }
            resp = requests.get(base, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            awards = (data.get("response") or {}).get("award") or []
            if not awards:
                break

            for a in awards:
                pi_first = a.get("piFirstName")
                pi_last = a.get("piLastName")
                if (not pi_first or not pi_last) and a.get("pdPIName"):
                    parts = a["pdPIName"].split()
                    if len(parts) >= 2:
                        pi_first = pi_first or parts[0]
                        pi_last = pi_last or " ".join(parts[1:])

                rows.append({
                    "id": str(a.get("id") or ""),
                    "agency": a.get("agency") or "NSF",
                    "awardeeName": a.get("awardeeName") or org,
                    "awardeeCity": a.get("awardeeCity"),
                    "awardeeStateCode": a.get("awardeeStateCode"),
                    "piFirstName": pi_first,
                    "piLastName": pi_last,
                    "title": a.get("title"),
                    "date": a.get("date"),
                    "startDate": a.get("startDate"),
                    "expDate": a.get("expDate"),
                    "fundsObligatedAmt": a.get("fundsObligatedAmt"),
                    "abstractText": a.get("abstractText"),
                    "orgCandidate": org    # <--- new column
                })

            if len(awards) < rpp:
                break
            offset += rpp

    df = pd.DataFrame(rows)
    return df


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    # nicer console printing for long abstracts
    pd.set_option("display.max_colwidth", 200)

    try:
        rows = search_orgs()
        df = to_df(rows)
        print(f"Total NSF awards found: {len(df)}")
        if df.empty:
            print(f"No NSF awards matched Corewell/Spectrum/Beaumont in the last {YEARS_BACK} year(s).")
        else:
            # show a peek without flooding the terminal
            print(df.head(25).to_string(index=False))
    except requests.HTTPError as e:
        print(f"HTTP error: {e}")   # e.g., maintenance window
        df = pd.DataFrame()
    except RuntimeError as e:
        print(f"API reported an error: {e}")
        df = pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error: {e}")
        df = pd.DataFrame()

    # --- save results safely ---
    out_path = f"data/nsf_awards_{YEARS_BACK}y.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if not df.empty:
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df)} rows to {out_path}")
    else:
        print("Nothing to save.")