"""
This DAG shows how to use the DuckDB package directly in a @task decorated task.
"""

from airflow.sdk import dag, task, Asset
from pendulum import datetime
import pandas as pd

@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_create():
    @task
    def create_pandas_df():
        "Create a pandas DataFrame with toy data and return it."
        ducks_in_my_garden_df = pd.DataFrame(
            {"colors": ["blue", "red", "yellow"], "numbers": [2, 3, 4]}
        )
        return ducks_in_my_garden_df

    @task(outlets=Asset('ducks_garden'))
    def create_duckdb_table_from_pandas_df(ducks_in_my_garden_df: pd.DataFrame):
        "Create a table in DuckDB based on a pandas DataFrame and query it"
        import duckdb
        conn = duckdb.connect("include/my_garden_ducks.db")
        conn.sql(
            """CREATE TABLE IF NOT EXISTS ducks_garden AS 
            SELECT * FROM ducks_in_my_garden_df;"""
        )

        sets_of_ducks = conn.sql("SELECT numbers FROM ducks_garden;").fetchall()
        for ducks in sets_of_ducks:
            print("quack " * ducks[0])

    create_duckdb_table_from_pandas_df(ducks_in_my_garden_df=create_pandas_df())


duckdb_create()
