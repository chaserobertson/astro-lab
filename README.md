# astro-lab
Astronomer demo

## Meta
The main branch of this repository is integrated with an Astronomer deployment, such that new commits pushed to main are automatically deployed.

Email alerts are configured for any DAG failures in the deployment.

## Dependencies

- Astronomer Cosmos
- DuckDB
- dbt
- pandas

## DAGS

- `duckdb_create`: A simple serial process that showcases different ways to connect to DuckDB
- `example_astronauts`: Included with deployment initialization conducted via `astro dev init`
- `my_simple_dbt_dag`: A customized combination of the Cosmos and dbt `jaffle_shop` tutorial setups
  - This DAG is triggered by completion of specific tasks in the duckdb and astronaut DAGs, though there is no actual data dependency that calls for it
- `nhl`: Pulls weekly schedules from the NHL API, aggregating the key details of each game into one summary, along with some no-op task spaghetti to make the graph look really wild
