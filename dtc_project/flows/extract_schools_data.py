"""Getting row schools data from data portal, clean and save to datalake."""

import os

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect_gcp.cloud_storage import DataFrameSerializationFormat, GcsBucket

latitude_column = 'Lat'
longitude_column = 'Long'

start_filter = '?$limit=100000'
schema = {
    'the_geom': 'string',
    'school_id': 'int64',
    'short_name': 'string',
    'address': 'string',
    'grade_cat': 'string',
    'lat': 'float64',
    'long': 'float64',
}
RAW_DATA_SCHOOLS_URL = 'RAW_DATA_SCHOOLS_URL'
GCS_BUCKET_BLOCK_NAME = 'GCS_BUCKET_BLOCK_NAME'
GCS_BUCKET_SCHOOLS_FILE_NAME = 'GCS_BUCKET_SCHOOLS_FILE_NAME'
GCS_BUCKET_SCHOOLS_PATH = 'GCS_BUCKET_SCHOOLS_PATH'


@task(name='Ingest schools data')
def get_schools() -> pd.DataFrame:
    """Extract row schools data from URL."""
    logger = get_run_logger()
    logger.info('INFO: Starting ingesting schools data for')

    if RAW_DATA_SCHOOLS_URL in os.environ():
        url = os.environ(RAW_DATA_SCHOOLS_URL)
    else:
        url = 'https://data.cityofchicago.org/resource/gqgn-ekwj.csv'

    data_url = '{u}{f}'.format(u=url, f=start_filter)
    df = pd.read_csv(data_url)

    logger.info('INFO: Ingesting schools data fcomplete')

    return df


@task(name='Clean row schools data')
def clean_schools(df: pd.DataFrame) -> pd.DataFrame:
    """Clean row schools data and apply schema."""
    logger = get_run_logger()
    logger.info('INFO: Starting cleaning schools data')

    df = df[df[[latitude_column]].notnull().all(1)]
    df = df[df[[longitude_column]].notnull().all(1)]
    df = df.astype(schema)

    logger.info('INFO: Finishing cleaning schools data')
    return df


@task(name='Write schools data to GCS')
def write_schools_to_gcs(df: pd.DataFrame) -> pd.DataFrame:
    """Write schools data to GCS bucket in parquet format."""
    logger = get_run_logger()
    logger.info('INFO: Starting upload schools data to GCS')

    if GCS_BUCKET_SCHOOLS_PATH in os.environ():
        to_path_place = os.environ(GCS_BUCKET_SCHOOLS_PATH)
    else:
        to_path_place = 'data/'

    if GCS_BUCKET_SCHOOLS_FILE_NAME in os.environ():
        to_path_file = os.environ(GCS_BUCKET_SCHOOLS_FILE_NAME)
    else:
        to_path_file = 'chicago_schools'

    if GCS_BUCKET_BLOCK_NAME in os.environ():
        bucket_block = os.environ(GCS_BUCKET_BLOCK_NAME)
    else:
        bucket_block = 'DTC-DE-BUCKET-BLOCK'

    to_path = '{p}{f}.parquet'.format(
        p=to_path_place,
        f=to_path_file,
    )
    gcs_bucket = GcsBucket.load(os.environ(bucket_block))
    gcs_bucket.upload_from_dataframe(
        df=df,
        to_path=to_path,
        serialization_format=DataFrameSerializationFormat.PARQUET,
    )
    logger.info('INFO: Uploading schools data to GCS complete')


@flow(name='Ingest row data to GCS Bucket')
def extract_schools() -> None:
    """Ingest row schools data."""
    logger = get_run_logger()
    logger.info('INFO: Starting ingesting schools data')
    row_df = get_schools()
    clean_df = clean_schools(row_df)
    write_schools_to_gcs(clean_df)
    logger.info('INFO: Ingesting schools data complete')


if __name__ == '__main__':
    extract_schools()
