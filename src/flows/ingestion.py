import os
import sys
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, "..", ".."))
sys.path.append(parent_path)
from src.logger import setup_logging
from src.data_ingestion.api_ingestion import main as api_ingestion
from src.data_ingestion.minio_to_snowflake import minio_raw_data_to_snowflake


@task(name="api_ingestion", retries=3, retry_delay_seconds=60)
def run_api_ingestion():
    """Prefect task to run API ingestion and log progress."""
    logger = setup_logging()
    logger.info("Starting API ingestion task")
    try:
        result = api_ingestion()
        logger.info(f"API ingestion completed: {result}")
        return result
    except Exception as e:
        logger.error(f"API ingestion failed: {e}")
        raise


@task(name="minio_to_snowflake", retries=3, retry_delay_seconds=60)
def run_minio_to_snowflake():
    """Prefect task to load data from MinIO to Snowflake and log progress."""
    logger = setup_logging()
    logger.info("Starting MinIO to Snowflake ingestion task")
    try:
        result = minio_raw_data_to_snowflake()
        logger.info(f"MinIO to Snowflake ingestion completed: {result}")
        return result
    except Exception as e:
        logger.error(f"MinIO to Snowflake ingestion failed: {e}")
        raise


@flow(name="data-ingestion-pipeline", description="Raw data ingestion pipeline - runs every day")
def data_ingestion_flow():
    """Main data ingestion flow orchestrating API ingestion and MinIO to Snowflake transfer."""
    
    api_result = run_api_ingestion()

    snowflake_result = run_minio_to_snowflake(wait_for=[api_result])
    
    return {"api_result": api_result, "snowflake_result": snowflake_result}


# explicit deployment (instead of automatic discovery from a dags/ folder)
if __name__ == "__main__":
    data_ingestion_flow.serve(
        name="daily-data-ingestion",
        schedule=CronSchedule(cron="0 0 * * *"),  # Daily at midnight
        tags=["data-ingestion", "daily"]
    )