"""
This DAG shows how to use the DuckDBHook in an Airflow task.
"""

from airflow.sdk import dag, task, Asset
from pendulum import datetime
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

@dag(
    start_date=datetime(2023, 6, 1), 
    schedule=Asset('ducks_garden'), 
    catchup=False
)
def duckdb_read():
    @task
    def query_duckdb(my_table, conn_id):
        my_duck_hook = DuckDBHook.get_hook(conn_id)
        conn = my_duck_hook.get_conn()
        r = conn.execute(f"SELECT * FROM {my_table};").fetchall()
        print(r)
        return r

    query_duckdb(my_table="ducks_garden", conn_id="my_local_duckdb_conn")


duckdb_read()
