from os.path import join

import dask.dataframe as dd
import numpy as np
from numpy.random import randn
import pandas as pd
import pytest

import pqbqjoin as pbj


NUM_ROW = 10
NUM_PQ_FILES = 3
IDX_COLS = ["a", "b"]
DATA_COLUMNS = [f"x{i}" for i in range(NUM_PQ_FILES)]
BASE_PATH = "gs://andrew-scratch-bucket/tmp"
OUTPUT_PATH = join(BASE_PATH, "pqbq-test.parquet")
FILENAMES = [f"df{i}.parquet" for i in range(NUM_PQ_FILES)]
DATA_PATHS = [join(BASE_PATH, f) for f in FILENAMES]


@pytest.fixture(scope="session", autouse=True)
def make_test_data():
    idx = range(2*NUM_ROW)
    idx_a = idx[:NUM_ROW]
    idx_b = idx[NUM_ROW:]
    dfs = [
        pd.DataFrame({IDX_COLS[0]: idx_a, IDX_COLS[1]: idx_b, c: randn(NUM_ROW)})
        for c in DATA_COLUMNS
    ]
    for df, p in zip(dfs, DATA_PATHS):
        df.to_parquet(p)


def test_make_dataset_runs():
    columns_select = [[c] for c in DATA_COLUMNS]
    bq_table_names = [f"table{i}" for i in range(NUM_PQ_FILES)]

    pq_items = [pbj.ParquetItem(p, c, t) for (p, c, t) in zip(DATA_PATHS, columns_select, bq_table_names)]
    bqj = pbj.BQJoiner(
        parquet_items=pq_items,
        join_cols=IDX_COLS,
        gcp_project="figure-development-data",
        bq_dataset="plnet",
        dest_table="tmp",
        drop_tables_before_run=True
    )
    bqj.make_dataset(output_path=OUTPUT_PATH)


def test_make_dataset_correct():
    dfs_src = [pd.read_parquet(p) for p in DATA_PATHS]
    df_joined_bq = dd.read_parquet(OUTPUT_PATH).compute().set_index(IDX_COLS).sort_index()
    df_joined_src = pd.concat([df.set_index(IDX_COLS) for df in dfs_src], axis=1).sort_index()
    assert np.isclose(df_joined_bq, df_joined_src).all().all()
