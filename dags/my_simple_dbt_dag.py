"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.sdk import dag, chain, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt"

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml"
)

@dag(params={"my_name": "Daffy"}, max_active_tasks=1)
def my_simple_dbt_dag():
    @task
    def create_duckdb():
        "Create the dbt DB"
        import duckdb
        duckdb.connect("include/dbt/duck.db")

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args={
            "vars": '{"my_name": {{ params.my_name }} }',
        },
        default_args={"retries": 2},
    )

    query_table = SQLExecuteQueryOperator(
        task_id="query_table",
        conn_id="dbt_duckdb_conn",
        sql="SELECT * FROM customers",
    )

    chain(create_duckdb(), transform_data, query_table)


my_simple_dbt_dag()
