# pqbqjoin
A utility for joining parquet files in BigQuery. This library exists because joins are much faster
and more reliable in BigQuery than in Spark or Dask.

# Usage
This library uses the [bq command-line tool](https://cloud.google.com/bigquery/docs/bq-command-line-tool).
To join parquet files, we do the following:

1. Upload the parquet files to temporary tables in BigQuery.
2. Join the temporary tables in BigQuery on the join columns you specify.
3. Write the joined output to the cloud storage bucket you specify.

The `BQJoiner` orchestrates all the work. The parameters are

* **pq_paths**: List of parquet files to join.
* **suffixes**: Suffixes to append to each column from each parquet file. This is mandatory because
columns can't be duplicated in a BigQuery table.
* **join_cols**: List of columns to use for the join; note that the join columns must have the same
names in each parquet file.
* **bq_dataset**: Dataset to use for BigQuery.
* **gcp_project**: GCP project. Default is `figure-development-data`.
* **bq_bin_path**: Location of bq binary. Default is `/snap/bin/bq`.

# Example
In the example below, we merge predictions from three different models on `record_nb`. Each parquet file
contains about 150MM rows.
```python
from os.path import join

import pqbqjoin as pqbq

IDX_COL = "record_nb"
OUTPUT_PATH = "gs://andrew-scratch-bucket/tmp/pqbq_example.parquet"
BASE_PATH = "gs://plnet/campaign/5/prediction"
PATHS = {
    "cnn": join(BASE_PATH, "serve_pred_cnn.parquet"),
    "ff": join(BASE_PATH, "serve_pred_ff.parquet"),
    "gbm": join(BASE_PATH, "serve_pred_gbm.parquet")
}
BQ_DATASET = "plnet"

bqj = pqbq.BQJoiner(
    pq_paths=list(PATHS.values()),
    suffixes=[f"_{k}" for k in PATHS],
    join_cols=[IDX_COL],
    bq_dataset=BQ_DATASET,
)
bqj.make_dataset(output_path=OUTPUT_PATH)
```