# Asset Market Experiment Website

This is a live classroom website for your asset market experiment.

## What it does
- students log in with a display name and participant code
- researcher logs in with an admin password
- researcher starts each round live
- students submit one buy or sell order each round
- the market clears at one uniform clearing price
- portfolios update automatically
- screenshot-treatment messaging can start in later rounds
- results are stored in SQLite and can be exported as CSV

## Quick start

```bash
cd experiment_site
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Then open:

```text
http://localhost:8000
```

## Default logins
- Admin password: `research2026`
- Participant codes: `P01` to `P50`

## Change the password or student codes
You can override both with environment variables.

```bash
export ADMIN_PASSWORD="mysecurepassword"
export PARTICIPANT_CODES="P01,P02,P03,P04,P05,P06,P07,P08,P09,P10"
python app.py
```

## Deployment
This works well on Render, Railway, or any VPS that can run Python.

Start command:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## Files
- `app.py` — backend server and experiment logic
- `templates/index.html` — frontend
- `data/experiment.db` — saved experiment results

## Notes
- Reset clears the current state and wipes saved round results.
- The market uses a simple uniform-price clearing rule based on matched bids and asks.
- Full export is at `/export/full_results.csv`.
