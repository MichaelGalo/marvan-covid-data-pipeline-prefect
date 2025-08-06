import os
from minio import Minio
from minio.error import S3Error
import dotenv

from src.logger import setup_logging
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import snowflake.connector
import io


logger = setup_logging()
dotenv.load_dotenv()


def minio_raw_data_to_snowflake():

    try:

        minio_client = Minio(
            endpoint=os.getenv("MINIO_EXTERNAL_URL"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False,
        )
        logger.info("Connected to MinIO")

        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA_RAW"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        logger.info("Connected to Snowflake")

        objects_to_process = minio_client.list_objects(
            bucket_name=os.getenv("MINIO_BUCKET_NAME"), recursive=True
        )

        minio_data = []
        for obj in objects_to_process:
            if obj.object_name.endswith(".csv"):
                minio_data.append(obj.object_name)
                logger.info(f"Found CSV file: {obj.object_name}")

                try:
                    minio_response = minio_client.get_object(
                        bucket_name=os.getenv("MINIO_BUCKET_NAME"),
                        object_name=obj.object_name,
                    )
                    data = minio_response.read().decode("utf-8")
                    logger.info(f"Read data from {obj.object_name}")
                    minio_response.close()
                    minio_response.release_conn()
                    logger.info(f"Data from {obj.object_name} processed successfully")

                    df = pd.read_csv(io.StringIO(data))
                    logger.info(f"Transformed data from {obj.object_name} to DataFrame")

                    # Add a surrogate primary key column -- FIXME needs abstraction
                    df.insert(0, "id", range(1, len(df) + 1))
                    logger.info(f"Added surrogate primary key to DataFrame for {obj.object_name}")

                    result = write_pandas(
                        conn,
                        df,
                        table_name=f"{obj.object_name.split('.')[0].replace('-', '_').upper()}_RAW",
                        database=os.getenv("SNOWFLAKE_DATABASE"),
                        auto_create_table=True,
                        schema=os.getenv("SNOWFLAKE_SCHEMA_RAW"),
                        overwrite=True,
                    )
                except Exception as e:
                    logger.error(f"Error processing {obj.object_name}: {e}")

    except S3Error as e:
        logger.error(f"MinIO S3Error: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
