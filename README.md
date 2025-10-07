# Compfilter (prototype)

A tiny internal webapp to stream-filter a very large CSV and download the result.
This initial version supports filtering by **rechtsvorm**.

## Quickstart

cd Compfilter
python3 -m venv compenv
source compenv/bin/activate
pip install -r requirements.txt
python backend/app.py

Open http://localhost:3004
