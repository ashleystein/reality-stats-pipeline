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

show_partition = dg.StaticPartitionsDefinition(["bachelor", "bachelorette", "traitors"])
REALITY_CAST_COLS = ["name", "age", "hometown", "state", "job", "eliminated", "show", "season"]


def get_url_info(partition_key: str) -> dict:
    df = pd.read_csv(data_folder / "scrape_url.csv", dtype=str)
    show_slug = partition_key.lower()
    mask = df["show"].str.lower().str.replace(r"^the\s+", "", regex=True) == show_slug
    matching = df[mask]
    if matching.empty:
        raise ValueError(f"No entry in scrape_url.csv for show partition: '{partition_key}'")
    row = matching.iloc[0] # This only processes the first row in scrape_url.csv
    show = utils.remove_leading_chars(row["show"], "the")
    return {"show": show, "season": str(row["season"]), "url": str(row["url"])}


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


def html_to_raw_csv(html_info: dict) -> pd.DataFrame:

    df_list = pd.read_html(html_info["html_file"])
    
    df = df_list[0]
    df["show"] = html_info["show"]
    df["season"] = html_info["season"]
   
    return df


def raw_to_processed_csv(df: pd.DataFrame) -> pd.DataFrame:
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip().map(remove_references))
    return df

def split_hometown(hometown_series: pd.Series) -> pd.DataFrame:
    """Split 'City, State' into separate hometown and state columns."""
    split = hometown_series.str.rsplit(",", n=1, expand=True)
    split.columns = ["hometown", "state"]
    split["hometown"] = split["hometown"].str.strip()
    split["state"]    = split["state"].str.strip()
    return split


def upsert_to_reality_cast(df: pd.DataFrame) -> None:
    """Append new rows to reality_cast.csv, replacing existing rows for the same show and season."""
    show = df["show"].iloc[0]
    season = df["season"].iloc[0]

    existing = pd.read_csv(reality_cast_file, dtype=str)
    existing = existing[
        ~((existing["show"].str.lower() == show.lower()) &
          (existing["season"] == season))
    ]
    combined = pd.concat([existing, df], ignore_index=True)
    combined.to_csv(reality_cast_file, index=False)


def normalize(df: pd.DataFrame, show_label: str, col_map: dict, extra_cols: dict = None) -> pd.DataFrame:
    """
    Normalize a processed show CSV into the reality_cast schema.

    Args:
        df:          Raw processed DataFrame
        show_label:  Value to write into the 'show' column
        col_map:     Mapping of source column → target column name
        extra_cols:  Static columns to add (e.g. {"job": None} when show has no job column)
    """
    df = df.rename(columns=col_map)
    
    if "hometown" in df.columns and "state" not in df.columns:
        split = split_hometown(df["hometown"])
        df["hometown"] = split["hometown"]
        df["state"]    = split["state"]

    if extra_cols:
        for col, val in extra_cols.items():
            if col not in df.columns:
                df[col] = val

    df["show"] = show_label


    # Return only the target schema columns that exist
    cols = [c for c in REALITY_CAST_COLS if c in df.columns]
    return df[cols].astype(str)

BACHELORETTE_COL_MAP = col_map={"Name":"name", "Age":"age", "Hometown":"hometown", "Occupation":"job", "Outcome":"eliminated"}
TRAITORS_COL_MAP = {"finish": "eliminated"}


@dg.asset(partitions_def=show_partition)
def reality_cast(context: dg.AssetExecutionContext) -> None:
    show = context.partition_key
    url_info = get_url_info(show)
    result = get_wiki_html(url_info)
    raw_csv_df = html_to_raw_csv(result)
    df = raw_to_processed_csv(raw_csv_df)

    match show:
        case "bachelorette":
            df = normalize(df, show_label="The Bachelorette", col_map=BACHELORETTE_COL_MAP)
        case "bachelor":
            if list(df.columns) == list(range(len(df.columns))):
                df.columns = ["name", "age", "hometown", "job", "eliminated", "season", "show"]
            df = normalize(df, show_label=show, col_map={})
        case "traitors":
            df = normalize(df, show_label=show, col_map=TRAITORS_COL_MAP, extra_cols={"job": None})

    upsert_to_reality_cast(df)
    context.add_output_metadata({"rows": len(df)})



reality_cast_job = dg.define_asset_job(
    name="reality_cast_job",
    selection=[reality_cast],
    partitions_def=show_partition,
)
