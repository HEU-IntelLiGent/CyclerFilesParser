#%%
import os
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
 #%%

def retrieve_column_names(filepath:str):
    
    with open(filepath, "r") as f:
        f.readline()  # Skip line 1
        raw_header = f.readline()

    return raw_header.strip().replace(" ", "\t").split("\t")



def load_dataframe(filepath:str, column_names = list[str]):

    return pl.read_csv(
        filepath,
        separator="\t",
        has_header=False,
        skip_rows=2,           # Skip the first two lines
        new_columns=column_names,   # Use the cleaned header
        truncate_ragged_lines=True  # Optional safety
        )


def format_dataframe_to_bdf(df: pl.DataFrame):

    return df.select([
        pl.col('Test(Sec)').cum_max().alias('Test Time / s'),
        pl.col('DPt-Time')
            .str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S')
            .dt.replace_time_zone("CET")
            .dt.convert_time_zone("UTC")
            .dt.timestamp('ms')
            .alias('Unix Time / s')/1000,
        pl.col('Amps').alias('Current / A'),
        pl.col('Volts').alias('Voltage / V'),
        pl.col('Cyc#').alias('Cycle Count / 1'),
        pl.col('Step').alias('Step Index / 1'),
        pl.col('Amp-hr').alias('Cumulative Capacity / Ah'),
    ])

def save_df_to_parquet(df:pl.DataFrame, output_filepath:str):

    filename = output_filepath.rsplit('.', 1)[0]
    bdf_filename = f"{filename}.bdf.parquet"
    df.write_parquet(bdf_filename)

#%%

if __name__ == "__main__":
    root_file_path =  "./data/"
    filepaths = [root_file_path + filename for filename in os.listdir(root_file_path) if filename.endswith(".txt")]

    for filepath in filepaths:
        column_names = retrieve_column_names(filepath)
        df = load_dataframe(filepath=filepath, column_names=column_names)
        df_bdf = format_dataframe_to_bdf(df=df)
        save_df_to_parquet(df_bdf, output_filepath=filepath)
