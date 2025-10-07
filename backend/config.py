import os
from pathlib import Path
CSV_PATH = Path("/Users/yer/Downloads/SCP/Rawfiles/combined_results_fin_fixed_sample_correctsbi.csv")
CSV_DELIMITER = ';'
CSV_ENCODING = 'utf-8'
HOST = "127.0.0.1"
PORT = int(os.environ.get("PORT", "3004"))
DEBUG = True
