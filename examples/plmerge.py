from os.path import join
import time
from typing import Callable

import dask
import dask.dataframe as dd
import ray
from ray.util.dask import ray_dask_get

import pqbqjoin as pqbq


IDX_COL = "record_nb"
OUTPUT_PATH_BQ = "gs://andrew-scratch-bucket/tmp/pqbq_example_bq.parquet"
OUTPUT_PATH_DASK = "gs://andrew-scratch-bucket/tmp/pqbq_example_dask.parquet"
BASE_PATH = "gs://plnet/campaign/5/prediction"
PATHS = {
    "cnn": join(BASE_PATH, "serve_pred_cnn.parquet"),
    "ff": join(BASE_PATH, "serve_pred_ff.parquet"),
    "gbm": join(BASE_PATH, "serve_pred_gbm.parquet")
}


def timed(func: Callable) -> Callable:
    def wrapper(*arg, **kw):
        '''source: http://www.daniweb.com/code/snippet368.html'''
        t1 = time.time()
        func(*arg, **kw)
        t2 = time.time()
        elapsed = t2 - t1
        msg = f"{func.__name__} execution time: {elapsed}"
        print(msg)
    return wrapper


@timed
def merge_preds_pqbq() -> None:
    bqj = pqbq.BQJoiner(
        pq_paths=PATHS.values(),
        suffixes=[f"_{k}" for k in PATHS],
        join_cols=[IDX_COL],
        bq_dataset="plnet",
    )
    bqj.make_dataset(output_path=OUTPUT_PATH_BQ)


@timed
def merge_preds_dask() -> None:

    def _read_parquet(path, suffix) -> dd.DataFrame:
        df = dd.read_parquet(path, gather_statistics=False).repartition(partition_size="100MB")
        df = df.set_index(IDX_COL)
        df.columns = [f"{c}_{suffix}" for c in df.columns]
        return df

    ray.shutdown()
    ray.init()
    dask.config.set(scheduler=ray_dask_get)
    df_t = [_read_parquet(path=p, suffix=s) for s, p in PATHS.items()]
    df = dd.concat(df_t, axis=1)
    df.to_parquet(OUTPUT_PATH_DASK)
    ray.shutdown()


def main():
    print("Merge using pqbq ...")
    merge_preds_pqbq()

    print("Merge using dask ...")
    merge_preds_dask()


if __name__ == "__main__":
    main()
