import requests
import sys
import os
from minio import Minio
from dotenv import load_dotenv

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, "..", ".."))
sys.path.append(parent_path)
from src.logger import setup_logging
import pandas as pd
import io

logger = setup_logging()

load_dotenv()

minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_url = os.getenv("MINIO_EXTERNAL_URL")
minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")
page_size = 365


def upload_to_minio(minio_client, bucket_name, data, object_name):
    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")
    minio_client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )


def get_time_series_data(url):
    data = []
    while url:
        logger.info(f"Requesting data from URL: {url}")
        try:
            response = requests.get(url, params={f"page_size": {page_size}})
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise e
        response_data = response.json()
        results = response_data["results"]
        next_url = response_data["next"]

        logger.info(f"Fetched {len(results)} records.")
        data.extend(results)
        url = next_url

    logger.info(f"Total records fetched: {len(data)}")
    data = pd.DataFrame(data)
    return data


def main():
    url = os.getenv("UK_API_URL")
    data = get_time_series_data(url)
    minio_client = Minio(
        minio_url,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )
    upload_to_minio(minio_client, minio_bucket_name, data, os.getenv("UK_CSV_NAME"))


if __name__ == "__main__":
    main()
