import ast
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "airflow" / "dags" / "traceai_daily_metrics.py"
COMPOSE_PATH = ROOT / "docker-compose.yml"
ENV_EXAMPLE_PATH = ROOT / ".env.example"
DB_INIT_PATH = ROOT / "database" / "init" / "00-create-airflow-database.sql"


def dag_call() -> tuple[ast.AST, dict[str, ast.expr]]:
    tree = ast.parse(DAG_PATH.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            expression = item.context_expr
            if (
                isinstance(expression, ast.Call)
                and isinstance(expression.func, ast.Name)
                and expression.func.id == "DAG"
            ):
                return tree, {keyword.arg: keyword.value for keyword in expression.keywords}
    raise AssertionError("DAG(...) call not found")


def test_dag_schedule_and_unpaused_creation() -> None:
    _, keywords = dag_call()

    assert ast.literal_eval(keywords["schedule"]) == "0 1 * * *"
    assert ast.literal_eval(keywords["catchup"]) is False
    assert ast.literal_eval(keywords["max_active_runs"]) == 1
    assert ast.literal_eval(keywords["is_paused_upon_creation"]) is False


def test_dag_start_date_uses_kolkata_timezone() -> None:
    tree, keywords = dag_call()
    start_date = keywords["start_date"]
    assert isinstance(start_date, ast.Call)

    tzinfo = next(keyword for keyword in start_date.keywords if keyword.arg == "tzinfo")
    assert isinstance(tzinfo.value, ast.Name)

    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == tzinfo.value.id
        ):
            assert isinstance(node.value, ast.Call)
            assert isinstance(node.value.func, ast.Name)
            assert node.value.func.id == "ZoneInfo"
            assert ast.literal_eval(node.value.args[0]) == "Asia/Kolkata"
            break
    else:
        raise AssertionError("DAG timezone assignment not found")


def test_compose_configures_scheduler_and_ui_timezone() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text())
    airflow_env = compose["services"]["airflow"]["environment"]

    assert airflow_env["AIRFLOW__CORE__DEFAULT_TIMEZONE"] == "Asia/Kolkata"
    assert airflow_env["AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE"] == "Asia/Kolkata"
    assert str(airflow_env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"]).lower() == "false"


def test_compose_unpauses_existing_dag_after_airflow_is_healthy() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text())
    init_service = compose["services"]["airflow-dag-init"]

    assert init_service["depends_on"]["airflow"]["condition"] == "service_healthy"
    assert init_service["command"] == ["bash", "/usr/local/bin/traceai-unpause-dag"]
    script = (ROOT / "airflow" / "unpause-dag.sh").read_text()
    assert 'airflow dags unpause "$dag_id"' in script


def test_airflow_database_defaults_are_consistent() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text())
    airflow_env = compose["services"]["airflow"]["environment"]
    env_example = dict(
        line.split("=", 1)
        for line in ENV_EXAMPLE_PATH.read_text().splitlines()
        if line and not line.startswith("#")
    )
    init_sql = DB_INIT_PATH.read_text()

    assert env_example["AIRFLOW_DB"] == "airflow"
    assert env_example["AIRFLOW_USER"] == "airflow"
    assert env_example["AIRFLOW_PASSWORD"] == "airflow_dev_password"

    connection = airflow_env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert "${AIRFLOW_USER:-airflow}" in connection
    assert "${AIRFLOW_PASSWORD:-airflow_dev_password}" in connection
    assert "${AIRFLOW_DB:-airflow}" in connection
    assert "@postgres:5432/" in connection

    assert re.search(r"CREATE USER airflow WITH PASSWORD 'airflow_dev_password';", init_sql)
    assert re.search(r"CREATE DATABASE airflow OWNER airflow;", init_sql)


def test_dag_upsert_is_idempotent() -> None:
    source = DAG_PATH.read_text()
    assert "ON CONFLICT (metric_date) DO UPDATE SET" in source
