#%%
import os
import json
import polars as pl
import plotly.graph_objects as go
from typing import Dict

import tempfile 
import shutil
from pathlib import Path

#%% Logging functions (directory-local)

def get_conversion_log_path(dirpath: str) -> str:
    return os.path.join(dirpath, "conversion_log.json")

def load_conversion_log(log_path: str) -> Dict[str, float]:
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            return json.load(f)
    return {}

def save_conversion_log(log_path: str, log_data: Dict[str, float]):
    with open(log_path, "w") as f:
        json.dump(log_data, f, indent=2)

#%% Core conversion functions

def retrieve_column_names(filepath: str):
    keywords = {"Rec#", "Cyc#", "State", "DPt-Time"}
    with open(filepath, "r") as f:
        for i, line in enumerate(f):
            word_list = line.strip().replace(" ", "\t").split("\t")
            if keywords.issubset(set(word_list)):
                return i, word_list
    raise ValueError(f"Column names {keywords} not found in file.")


def load_dataframe(filepath: str, column_names: list[str], skip_rows:int) -> pl.DataFrame:
    return pl.read_csv(
        filepath,
        separator="\t",
        has_header=False,
        skip_rows=skip_rows,
        new_columns=column_names,
        truncate_ragged_lines=True,
    )



def load_dataframe_large(filepath: str, column_names: list[str], chunk_rows: int = 500_000) -> pl.DataFrame:
    """
    Convert a large CSV to a single Parquet file without exhausting RAM.

    Reads the CSV in `chunk_rows` sized batches, converts each chunk to Parquet in
    a temporary directory, then consolidates them into one Parquet file. It returns
    a *LazyFrame* pointing to the combined dataset so you can keep working lazily
    and call `.collect()` only when you actually need the data in memory.

    - csv_path : path to the input CSV
    - out_path : path for the consolidated Parquet
    - chunk_rows : rows per chunk (tune to taste, default 500k)
    """

    tmp = Path(tempfile.mkdtemp())          # 1) scratch space for chunk files
    try:
        skip, part = 1, 0
        while True:                         # 2) stream CSV → small Parquet chunks
            df = pl.read_csv(
                filepath,
                separator="\t",
                has_header=False,
                skip_rows=skip + (1 if part==0 else -1),
                n_rows=chunk_rows,
                new_columns=column_names,
                truncate_ragged_lines=True,
            )
            if df.height == 0:              # no more rows → stop
                break
            df.write_parquet(tmp / f"{part:05}.parquet")
            skip += df.height
            part = part + 1

        # Build lazy concatenation and write final parquet
        lazy_df = pl.concat(
            pl.scan_parquet(p) for p in sorted(tmp.glob("*.parquet"))
        )
        #lazy_df.sink_parquet(out_path, compression="zstd")
        return lazy_df
    finally:
        shutil.rmtree(tmp)                  # 4) clean up




def format_dataframe_to_bdf(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([
        pl.col('Test(Sec)').cum_max().alias('Test Time / s'),
        pl.col('DPt-Time')
            .str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S')
            .dt.replace_time_zone("CET")
            .dt.convert_time_zone("UTC")
            .dt.timestamp('ms')
            .alias('Unix Time / s') / 1000,
        pl.col('Amps').alias('Current / A'),
        pl.col('Volts').alias('Voltage / V'),
        pl.col('Cyc#').alias('Cycle Count / 1'),
        pl.col('Step').alias('Step Index / 1'),
        pl.col('Amp-hr').alias('Cumulative Capacity / Ah'),
    ])

def save_df_to_parquet(df: pl.DataFrame, output_filepath: str) -> str:
    filename = output_filepath.rsplit('.', 1)[0]
    bdf_filename = f"{filename}.bdf.parquet"
    df.write_parquet(bdf_filename)
    return bdf_filename

#%% Optional: Plotting function

def plot_bdf_quantities(df: pl.DataFrame, title: str = "Battery Data"):
    fig = go.Figure()
    x_axis = df['Unix Time / s'] if 'Unix Time / s' in df.columns else df['Test Time / s']
    x_label = 'Unix Time / s' if 'Unix Time / s' in df.columns else 'Test Time / s'
    for col in df.columns:
        if col != x_label:
            fig.add_trace(go.Scatter(x=x_axis, y=df[col], mode='lines', name=col))
    fig.update_layout(title=title, xaxis_title=x_label, yaxis_title="Value", height=600, width=1000)
    fig.show()

#%% Main runner

if __name__ == "__main__":
    root_file_path = r"C:\Users\eibarc\Downloads\OneDrive_2025-07-18\PAA unmodified_B-37-01"

    for dirpath, _, filenames in os.walk(root_file_path):
        log_path = get_conversion_log_path(dirpath)
        log = load_conversion_log(log_path)
        updated_log = log.copy()

        for filename in filenames:
            if not filename.endswith(".txt"):
                continue

            filepath = os.path.join(dirpath, filename)
            mod_time = os.path.getmtime(filepath)
            rel_path_key = os.path.relpath(filepath, dirpath)

            if rel_path_key in log and log[rel_path_key] == mod_time:
                print(f"Skipping (unchanged): {filepath}")
                continue

            print(f"Processing: {filepath}")
            try:
                header_location, column_names = retrieve_column_names(filepath)
                df = load_dataframe(filepath=filepath, column_names=column_names, skip_rows=header_location+1)
                df_bdf = format_dataframe_to_bdf(df=df)
                save_df_to_parquet(df_bdf, output_filepath=filepath)
                updated_log[rel_path_key] = mod_time
            except Exception as e:
                print(f"Failed to process {filepath}: {e}")

        save_conversion_log(log_path, updated_log)
