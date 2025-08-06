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


@task(name="dbt_test_cleaning", retries=1, retry_delay_seconds=120)
def run_dbt_test_cleaning():
    """Prefect task to run dbt test for cleaning models and log progress."""
    logger = setup_logging()
    logger.info("Starting dbt test cleaning task")
    try:
        command = f"cd {DBT_PROJECT_DIR} && dbt test --select models/cleaned"
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"dbt test cleaning completed successfully: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt test cleaning failed: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"dbt test cleaning failed: {e}")
        raise


@task(name="dbt_run_cleaning", retries=1, retry_delay_seconds=120)
def run_dbt_run_cleaning():
    """Prefect task to run dbt cleaning models and log progress."""
    logger = setup_logging()
    logger.info("Starting dbt run cleaning task")
    try:
        command = f"cd {DBT_PROJECT_DIR} && dbt run --select models/cleaned"
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"dbt run cleaning completed successfully: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run cleaning failed: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"dbt run cleaning failed: {e}")
        raise


@flow(name="dbt-cleaning-pipeline", description="dbt cleaning pipeline - runs daily")
def dbt_cleaning_flow():
    """Main dbt cleaning flow orchestrating debug, test, and run tasks."""
    
    debug_result = run_dbt_debug()
    test_result = run_dbt_test_cleaning(wait_for=[debug_result])
    run_result = run_dbt_run_cleaning(wait_for=[test_result])
    
    return {
        "debug_result": debug_result,
        "test_result": test_result,
        "run_result": run_result
    }


# Deployment with daily schedule
if __name__ == "__main__":
    dbt_cleaning_flow.serve(
        name="daily-dbt-cleaning",
        schedule=CronSchedule(cron="0 2 * * *"),  # Daily at 2 AM (after staging at 1 AM and after ingestion at midnight)
        tags=["dbt", "cleaning", "daily"]
    )