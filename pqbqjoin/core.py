from dataclasses import dataclass
import logging
import multiprocessing as mp
import os
import re
import subprocess
from typing import Sequence
import uuid

import dask.dataframe as dd

BQ_MAX_THREADS = 4


logger = logging.getLogger(__name__)


def _random_str() -> str:
    return 'delete_' + re.sub(r'\W+', '', str(uuid.uuid4()))


@dataclass
class ParquetItem:
    """
    Parameters
    ----------
    data_path: path to parquet file
        Must be like gs://

    Attributes
    ----------
    columns_select: columns to select, excluding idx cols
    columns_alias: aliases for columns_select
    bq_table_name: table destination for parquet data
    """
    data_path: str
    idx_cols: Sequence[str]
    suffix: str = ""

    def __post_init__(self):
        self.columns_select = [
            c for c in dd.read_parquet(self.data_path).columns if c not in self.idx_cols
        ]
        self.bq_table_name = _random_str()
        self.columns_alias = [f"{c}{self.suffix}" for c in self.columns_select]


def _sh_run(cmd: str) -> None:
    """
    Run a shell command.
    """
    subprocess.run(cmd, shell=True, check=True)


# pylint: disable=R0903
class BQJoiner:
    """
    Process
    -------
    1. Take a set of parquet files, upload them to separate tables in BQ.
    2. Join tables in BQ and store output in gs://
    3. Clean up tables created during the process.

    Parameters
    ----------
    pq_paths: paths to parquet files
    suffixes: suffixes to append to join output
    join_cols: columns to use for join
        Note: The columns we're using for joins must be the same in each parquet file.
    bq_dataset: BQ dataset
    gcp_project: GCP project
    bq_bin_path: Result of 'bq --version'
    drop_tables_before_run: Drop all tables (if they exist) before attempting to create them
    """
    def __init__(
        self,
        pq_paths: Sequence[str],
        suffixes: Sequence[str],
        join_cols: Sequence[str],
        bq_dataset: str,
        gcp_project: str = "figure-development-data",
        bq_bin_path: str = "/snap/bin/bq",
        drop_tables_before_run: bool = False
    ):
        if len(pq_paths) != len(suffixes):
            raise ValueError("Parameters pq_paths and suffixes must have the same length")

        self.join_cols = join_cols
        self.parquet_items = [
            ParquetItem(data_path=p, idx_cols=join_cols, suffix=s) for p, s in zip(pq_paths, suffixes)
        ]
        self.gcp_project = gcp_project
        self.bq_dataset = bq_dataset
        self.dest_table = _random_str()
        self.bq_bin_path = bq_bin_path
        self.drop_tables_before_run = drop_tables_before_run

    def _compute_schema_table_name(self, table_name: str, issql: bool) -> str:
        if issql:
            return f"`{self.gcp_project}.{self.bq_dataset}.{table_name}`"
        return f"{self.gcp_project}:{self.bq_dataset}.{table_name}"

    def _upload_pq_files_to_bq(self) -> None:
        create_cmds = [
            f"{self.bq_bin_path} load --source_format=PARQUET {self.bq_dataset}.{pi.bq_table_name} "
            f"{pi.data_path}" for pi in self.parquet_items
        ]
        with mp.get_context("spawn").Pool(processes=BQ_MAX_THREADS) as pool:
            create_jobs_apply = [pool.apply_async(_sh_run, args=(c,)) for c in create_cmds]
            create_jobs_run = [p.get() for p in create_jobs_apply]
        del create_jobs_run

    def _compute_bq_join_query(self) -> str:

        def make_table_alias(pi: ParquetItem) -> str:
            table_name_sql = self._compute_schema_table_name(pi.bq_table_name, issql=True)
            return f"{table_name_sql} {pi.bq_table_name}"

        pi_base = self.parquet_items[0]

        # select
        select_clause_index = [
            f"{pi_base.bq_table_name}.{c}" for c in self.join_cols
        ]
        select_clause_columns = [
            f"{pi.bq_table_name}.{c_orig} {c_alias}"
            for pi in self.parquet_items
            for (c_orig, c_alias) in zip(pi.columns_select, pi.columns_alias)
        ]
        select_clause = "select\n" + ",\n".join(select_clause_index + select_clause_columns)

        from_clause = f"from {make_table_alias(pi_base)}"

        # join
        join_table_t = [
            f"inner join {make_table_alias(pi)}"
            for pi in self.parquet_items[1:]
        ]

        on_conds_t = [
            "on " + " and ".join([f"{pi.bq_table_name}.{c} = {pi_base.bq_table_name}.{c}" for c in self.join_cols])
            for pi in self.parquet_items[1:]
        ]

        join_clause_t = sum(zip(join_table_t, on_conds_t), ())
        join_clause = "\n".join(join_clause_t)

        query = "\n".join((select_clause, from_clause, join_clause))
        return query

    def _join_bq_data(self) -> None:
        query = self._compute_bq_join_query()

        dest_table_sh = self._compute_schema_table_name(self.dest_table, issql=False)
        join_cmd = (
            f"{self.bq_bin_path} query --use_legacy_sql=false "
            f"--destination_table={dest_table_sh} '{query}'"
        )
        _sh_run(join_cmd)

    def _export_bq_dataset(self, output_path: str) -> None:
        path, extension = os.path.splitext(output_path)
        dest_table_sh = self._compute_schema_table_name(self.dest_table, issql=False)
        output_path_wildcard = f"{output_path}/{path}-*{extension}"
        cmd = " ".join((
            "bq extract --destination_format PARQUET --compression SNAPPY",
            dest_table_sh,
            output_path_wildcard
        ))
        _sh_run(cmd)

    def _drop_bq_tables(self) -> None:
        """
        Drop all BQ tables created during the process.
        """
        logger.info("Dropping existing tables, if they exist ...")
        with mp.get_context("spawn").Pool(processes=BQ_MAX_THREADS) as pool:
            drop_jobs = [
                pool.apply_async(_drop_table, args=(self, pi.bq_table_name))
                for pi in self.parquet_items
            ]
            drop_jobs.append(
                pool.apply_async(_drop_table, args=(self, self.dest_table))
            )
            drop_jobs = [p.get() for p in drop_jobs]

    def make_dataset(self, output_path: str) -> None:
        logger.info("Starting pq-bq join process ...")
        if self.drop_tables_before_run:
            self._drop_bq_tables()

        try:
            logger.info("Uploading parquet files to bq ...")
            self._upload_pq_files_to_bq()
            logger.info("Joining data in bq ...")
            self._join_bq_data()
            logger.info("Writing joined data to %s ..", output_path)
            self._export_bq_dataset(output_path=output_path)
        except Exception as e:
            self._drop_bq_tables()
            raise e
        self._drop_bq_tables()
        logger.info("Done.")


# pylint: disable=W0212
def _drop_table(bqj: BQJoiner, table_name: str):
    """
    Factored out of BQJoiner for multiprocessing.
    """
    table_name = bqj._compute_schema_table_name(table_name, issql=True)
    drop_cmd = f"{bqj.bq_bin_path} query --use_legacy_sql=false 'drop table if exists {table_name}'"
    _sh_run(drop_cmd)
