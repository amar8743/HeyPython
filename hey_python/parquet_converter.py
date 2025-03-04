import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_csv_to_parquet(csv_filepath, parquet_filepath, schema=None):
    """
    Converts a CSV file to Parquet format, allowing specification of schema.

    Args:
        csv_filepath (str): Path to the input CSV file.
        parquet_filepath (str): Path to the output Parquet file.
        schema (pyarrow.Schema, optional): PyArrow schema to enforce. 
                                          If None, schema is inferred from CSV.
    """
    df = pd.read_csv(csv_filepath)

    if schema:
        table = pa.Table.from_pandas(df, schema=schema)
    else:
        table = pa.Table.from_pandas(df)

    pq.write_table(table, parquet_filepath)