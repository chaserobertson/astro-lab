"""
This DAG shows how its possible to use either the DuckDB python sdk or airflow connection to access a duck DB.
"""

from airflow.sdk import dag, task, chain, Asset
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime
import pandas as pd

TABLE_NAME = "color_rank"
DUCKDB_FILE = f"include/{TABLE_NAME}.db"
DUCKDB_CONN_ID = "my_local_duckdb_conn"

@dag(
    start_date=datetime(2023, 6, 1),
    schedule=CronTriggerTimetable("0 18 * * *", timezone="UTC"),
    catchup=False
)
def duckdb_create():
    @task
    def create_pandas_df():
        "Create a pandas DataFrame with toy data and return it."
        import random
        colors = ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]
        df = pd.DataFrame(
            {"color": colors, "number": [random.randint(1, 15) for c in colors]}
        )
        return df

    @task(outlets=[Asset("color_rank")])
    def create_duckdb_table_from_pandas_df(df: pd.DataFrame):
        "Use the sdk to create a table in DuckDB based on a pandas DataFrame and query it"
        import duckdb
        conn = duckdb.connect(DUCKDB_FILE) # creates a new DB file if it doesn't already exist
        conn.sql(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} AS SELECT * FROM df;") # df accessible in sql via in-memory magic

        sets_of_ducks = conn.sql(f"SELECT color, number FROM {TABLE_NAME} ORDER BY number DESC;").fetchall()
        for color, rank in sets_of_ducks:
            print(f"{color}: " + "quack " * rank)

    @task
    def query_duckdb():
        "Use a connection and hook to retrieve the contents of the table"
        from duckdb_provider.hooks.duckdb_hook import DuckDBHook
        my_duck_hook = DuckDBHook.get_hook(DUCKDB_CONN_ID)
        conn = my_duck_hook.get_conn()
        r = conn.execute(f"SELECT * FROM {TABLE_NAME};").fetchall()
        return r
    
    @task
    def use_query_results(query_results):
        print(query_results)

    df = create_pandas_df()
    db = create_duckdb_table_from_pandas_df(df)
    q = query_duckdb()
    res = use_query_results(q)

    chain(df, db, q, res)

duckdb_create()
