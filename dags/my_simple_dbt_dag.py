"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.sdk import dag, chain, task, Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt"

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml" # path to db set in profiles
)

@dag(
    params={"my_name": "Daffy"},
    schedule=Asset('color_rank') & Asset('current_astronauts')
)
def my_simple_dbt_dag():
    @task
    def create_duckdb():
        "Ensure the DB exists before running dbt"
        import duckdb
        duckdb.connect("include/dbt/duck.db")

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args={
            "vars": '{"my_name": {{ params.my_name }} }',
            "queue": "dbt",
        },
        default_args={"retries": 5, "retry_delay": 10},
    )

    do_other_stuff = EmptyOperator(task_id="do_other_stuff")

    query_table = SQLExecuteQueryOperator(
        task_id="query_table",
        conn_id="dbt_duckdb_conn",
        sql="SELECT * FROM customers",
    )

    chain(create_duckdb(), [transform_data, do_other_stuff], query_table)


my_simple_dbt_dag()
