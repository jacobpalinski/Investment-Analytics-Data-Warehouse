''' Import Modules '''
import os
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from airflow.dags.utils.s3_utils import S3

def extract_listed_tickers():
    '''
    Extracts the latest Nasdaq listed tickers from a public Github repository and uploads data to S3.
    '''
    # Create setup for logging
    logger = logging.getLogger(__name__)

    # Load environment variables
    load_dotenv()

    # Initantiates S3 class
    s3_client = S3(aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

    # Nasdaq Listings Raw Github Url
    csv_url = 'https://raw.githubusercontent.com/datasets/nasdaq-listings/refs/heads/main/data/nasdaq-listed.csv'

    # Filename for listed symbols containing current date
    filename = f"nasdaq_listed_symbols_{datetime.now().strftime('%Y%m%d')}.csv"

    # Make Request
    response = requests.get(csv_url)

    # Create new file in S3 if request is successful and response is non empty and update metadata
    if response.status_code == 200 and len(response.content) > 0:
        try: 
            s3_client.put_object(
                bucket=os.getenv('AWS_S3_BUCKET'),
                key=filename,
                data=response.content
            )
            s3_client.update_metadata(bucket=os.getenv('AWS_S3_BUCKET'), metadata_object='metadata.json', metadata_key='nasdaq_listed_tickers')
        except Exception as e: 
            logger.exception(f"Failed to extract nasdaq listed tickers: {e}")

if __name__ == "__main__":
    extract_listed_tickers()