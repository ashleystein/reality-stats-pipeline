from datetime import datetime, timedelta
from pathlib import Path

import dagster as dg
import pandas as pd

from src import utils
from src.utils import remove_references

REPO_ROOT        = Path(__file__).resolve().parents[4]
data_folder      = REPO_ROOT / "data"
html_folder      = REPO_ROOT / "data" / "html_raw"
raw_folder       = REPO_ROOT / "data" / "csv_raw"
processed_folder = REPO_ROOT / "data" / "processed"
reality_cast_file = REPO_ROOT / "data" / "reality_cast.csv"

def get_wiki_html(url_info: dict) -> dict:
    show = url_info["show"]
    season = url_info["season"]
    url = url_info["url"]

    file_name = html_folder / f"{show}_{season}"
    if utils.does_file_exist(file_name):
        last_modified_time = utils.get_last_modified_time(file_name)
        if datetime.now() - datetime.fromtimestamp(last_modified_time) < timedelta(days=3):
            print(f"{file_name} already exists, returning location.")
            return {"html_file": str(file_name), "show": show, "season": season}
    soup = utils.scrapePage(url)
    text = soup.find_all("table", class_="wikitable sortable")
    html_folder.mkdir(parents=True, exist_ok=True)
    with open(file_name, "w") as f:
        f.write(str(text))
    print(f"Saved to {file_name}")
    return {"html_file": str(file_name), "show": show, "season": season}