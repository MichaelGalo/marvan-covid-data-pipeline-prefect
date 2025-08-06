import os
import sys
import subprocess
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, "..", ".."))
sys.path.append(parent_path)
from src.logger import setup_logging


# dbt project paths
project_root = os.path.abspath(os.path.join(current_path, "..", ".."))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt", "marvan_covid")
DBT_PROFILES_DIR = os.path.join(project_root, "dbt", "marvan_covid")


@task(name="dbt_debug", retries=1, retry_delay_seconds=120)
def run_dbt_debug():
    """Prefect task to run dbt debug and log progress."""
    logger = setup_logging()
    logger.info("Starting dbt debug task")
    try:
        command = f"cd {DBT_PROJECT_DIR} && dbt debug"
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"dbt debug completed successfully: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt debug failed: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"dbt debug failed: {e}")
        raise


@task(name="dbt_test_staging", retries=1, retry_delay_seconds=120)
def run_dbt_test_staging():
    """Prefect task to run dbt test for staging models and log progress."""
    logger = setup_logging()
    logger.info("Starting dbt test staging task")
    try:
        command = f"cd {DBT_PROJECT_DIR} && dbt test --select models/staged"
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"dbt test staging completed successfully: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt test staging failed: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"dbt test staging failed: {e}")
        raise


@task(name="dbt_run_staging", retries=1, retry_delay_seconds=120)
def run_dbt_run_staging():
    """Prefect task to run dbt staging models and log progress."""
    logger = setup_logging()
    logger.info("Starting dbt run staging task")
    try:
        command = f"cd {DBT_PROJECT_DIR} && dbt run --select models/staged"
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"dbt run staging completed successfully: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run staging failed: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"dbt run staging failed: {e}")
        raise


@flow(name="dbt-staging-pipeline", description="dbt staging pipeline - runs daily")
def dbt_staging_flow():
    """Main dbt staging flow orchestrating debug, test, and run tasks."""
    
    debug_result = run_dbt_debug()
    test_result = run_dbt_test_staging(wait_for=[debug_result])
    run_result = run_dbt_run_staging(wait_for=[test_result])
    
    return {
        "debug_result": debug_result,
        "test_result": test_result,
        "run_result": run_result
    }


# Deployment with daily schedule (equivalent to schedule_interval=timedelta(days=1))
if __name__ == "__main__":
    dbt_staging_flow.serve(
        name="daily-dbt-staging",
        schedule=CronSchedule(cron="0 1 * * *"),  # Daily at 1 AM
        tags=["dbt", "staging", "daily"]
    )