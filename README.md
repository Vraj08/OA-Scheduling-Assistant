# OA Scheduler (Streamlit)

This app helps OAs **add**, **remove**, and **call-out** shifts using a Google Sheets schedule.

## Run locally

1. Create a virtualenv and install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Add Streamlit secrets:

- Copy `.streamlit/secrets.example.toml` to `.streamlit/secrets.toml`
- Fill in `SHEET_URL` and the `gcp_service_account` service account json fields

3. Start Streamlit:

```bash
streamlit run app.py
```

## Built-in recovery for internet/quota issues

If the app can't reach Google Sheets (internet off/DNS/VPN) or hits Google API rate limits (read/write quota),
it shows a **Recovery** panel with:

- Clear instructions on what to do (check wifi, disable VPN, etc.)
- **Retry now** button
- **Hard refresh** button (clears Streamlit caches + in-session read caches)
- On quota errors: a **30-second countdown** and automatic retry in the **same session** (your selections stay)

## Code layout

- `app.py`: tiny entrypoint
- `oa_app/ui/page.py`: Streamlit page (UI)
- `oa_app/ui/availability.py`: availability calculations + UI widgets
- `oa_app/integrations/gspread_io.py`: Google Sheets auth + backoff
- `oa_app/services/*`: non-UI services (roster, audit log)
