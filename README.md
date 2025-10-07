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

## SBI filtering

Use the **SBI** filter panel to target the `mainSBI`, `subSBI`, and `allSBI` columns individually.

- **Manual entry** – Type or paste the desired SBI codes into the textarea for the relevant section. You can separate codes with new lines, spaces, commas, or semicolons.
- **CSV upload** – Each section also lets you upload and reuse CSV/TXT files that contain codes. After uploading, pick the saved file from the dropdown to activate it for that section.

### CSV layout

Create a UTF‑8 encoded `.csv` (or `.txt`) file that lists one SBI code per row. A header row is optional. Only the first column is read.

```csv
code
70102
70201
73110
```

You can upload separate files for **Main SBI**, **Sub SBI**, and **All SBI** lists as needed.
