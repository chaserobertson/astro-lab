"""
## NHL API example DAG
"""

from airflow.sdk import dag, task, TriggerRule, chain
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime
import requests
import json
import pendulum

@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["nhl"],
)
def nhl():
    all_weeks = []
    dates = [
        week_stamp.strftime("%Y-%m-%d")
        for week_stamp in pendulum.interval(pendulum.now().subtract(months=4), pendulum.now()).range("weeks")
    ]
    for date_str in dates:
        @task(task_id=f"get_nhl_{date_str}")
        def get_nhl(date: str) -> list:
            "Get this week's game schedule from the nhl api (includes Olympic games!)"
            try:
                url = f"https://api-web.nhle.com/v1/schedule/{date}"
                print(f"requesting from url {url}")
                r = requests.get(url)
                r.raise_for_status()
                response = r.json()
            except Exception as e:
                print(f"Failed to fetch from nhl api, fallback to example file: {e}")
                with open("include/nhl_example.json", "r") as f:
                    response = json.load(f)
            week = response["gameWeek"]
            games = []
            for day in week:
                for game in day["games"]:
                    games.append(game)
            return games

        @task(task_id=f"process_game_{date_str}")
        def process_game(game: dict) -> dict:
            "Extract only the interesting details about each game"
            details = dict(
                start_time=game["startTimeUTC"],
                venue=game["venue"],
                game_state=game["gameState"],
                home_team=game["homeTeam"]["commonName"]["default"],
                away_team=game["awayTeam"]["commonName"]["default"],
                home_score=game["homeTeam"].get("score", 0),
                away_score=game["awayTeam"].get("score", 0),
            )
            return details

        week_results = process_game.expand(game=get_nhl(date_str))
        all_weeks.append(week_results)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summary(weeks: list) -> None:
        "Aggregate all weekly results into one summary"
        all_games = [game for week in weeks for game in week]
        all_games.sort(key=lambda g: g["start_time"])
        print(f"Total games across all weeks: {len(all_games)}")
        for game in all_games:
            date = pendulum.parse(game["start_time"]).format("YYYY-MM-DD")
            print(f"  {date}  {game['away_team']} @ {game['home_team']}: {game['away_score']}-{game['home_score']}")

    agg = summary(all_weeks)

    fin = EmptyOperator(task_id="fin")

    branches, ops = [], []
    for i, date_str in enumerate(dates):
        if i % 3 == 0:
            continue
        @task.branch(task_id=f"do_extra_{date_str}")
        def do_extra(true_task_id: str, false_task_id: str):
            return true_task_id if i % 2 == 0 else false_task_id
        branch = do_extra(f"extra_{date_str}", "fin")
        branches.append(branch)

        op = EmptyOperator(task_id=f"extra_{date_str}")
        ops.append(op)

    chain(agg, branches, ops, fin)
    chain(branches, list(reversed(ops)))
    chain(branches, sorted(ops, key=lambda x: x.__hash__()))

nhl()
