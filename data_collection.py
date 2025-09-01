import requests, datetime, pandas as pd

BASE_URL = "https://www.research.gov/awardapi-service/v1/awards.json"

# Go back 20 years to be safe
today = datetime.date.today()
start_date = (today - datetime.timedelta(days=365*20)).strftime("%m/%d/%Y")
end_date = today.strftime("%m/%d/%Y")

ORG_CANDIDATES = [
    "Corewell Health",
    "Beaumont Health",
    "William Beaumont Hospital",
    "Beaumont Hospital",
    "Spectrum Health",
    "Helen DeVos Children's Hospital",
    "Butterworth Hospital",
    "Lakeland Hospital",
    "Blodgett Hospital",
]

PRINT_FIELDS = ",".join([
    "id","title","awardeeName","piFirstName","piLastName",
    "startDate","expDate","abstractText","awardeeStateCode","awardeeCity"
])

def fetch(params):
    out = []
    offset = 1
    while True:
        p = dict(params, rpp=25, offset=offset, printFields=PRINT_FIELDS)
        r = requests.get(BASE_URL, params=p, timeout=30)
        r.raise_for_status()
        data = r.json()
        if "response" not in data or "award" not in data["response"]:
            break
        batch = data["response"]["award"]
        out.extend(batch)
        if len(batch) < 25:
            break
        offset += 25
    return out

all_rows = []

# 1) Exact org match via awardeeName
for org in ORG_CANDIDATES:
    all_rows += fetch({
        "awardeeName": org,           # exact org filter
        "dateStart": start_date,      # award date window
        "dateEnd": end_date
    })

# 2) Fuzzy fallback via keyword (phrase + unquoted)
for org in ORG_CANDIDATES:
    for term in (f"\"{org}\"", org):
        all_rows += fetch({
            "keyword": term,          # free-text across fields
            "dateStart": start_date,
            "dateEnd": end_date
        })

# 3) Michigan-only sweep to catch stray records that mention hospitals but not org field
all_rows += fetch({
    "awardeeStateCode": "MI",
    "keyword": "Beaumont OR Spectrum OR Corewell",
    "dateStart": start_date,
    "dateEnd": end_date
})

df = pd.DataFrame(all_rows).drop_duplicates(subset=["id"]) if all_rows else pd.DataFrame()

print(f"Total NSF awards found: {len(df)}")
if df.empty:
    print("No NSF awards matched these org names (last 20 years).")
else:
    cols = [c for c in ["id","title","awardeeName","piFirstName","piLastName","startDate","awardeeCity","awardeeStateCode"] if c in df.columns]
    print(df[cols].head(25).to_string(index=False))