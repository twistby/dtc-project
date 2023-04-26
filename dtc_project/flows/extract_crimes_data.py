"""Getting row crimes data from data portal, clean and save to datalake."""

import calendar
import os

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect_gcp.cloud_storage import DataFrameSerializationFormat, GcsBucket

latitude_column = 'latitude'
longitude_column = 'longitude'
years_default = [2022, 2023]
start_filter = '?$limit=100000'
schema = {
    'id': 'int64',
    'case_number': 'string',
    'date': 'datetime64[ns]',
    'block': 'string',
    'iucr': 'string',
    'primary_type': 'string',
    'description': 'string',
    'location_description': 'string',
    'arrest': 'bool',
    'domestic': 'bool',
    'beat': 'string',
    'district': 'string',
    'ward': 'string',
    'community_area': 'string',
    'fbi_code': 'string',
    'x_coordinate': 'float64',
    'y_coordinate': 'float64',
    'year': 'int64',
    'updated_on': 'datetime64[ns]',
    'latitude': 'float64',
    'longitude': 'float64',
    'location': 'string',
}

GCS_BUCKET_BLOCK_NAME = 'GCS_BUCKET_BLOCK_NAME'
GCS_BUCKET_CRIMES_PATH = 'GCS_BUCKET_CRIMES_PATH'
GCS_BUCKET_CRIMES_FILE_NAME = 'GCS_BUCKET_CRIMES_FILE_NAME'

RAW_DATA_CRIMES_URL = 'RAW_DATA_CRIMES_URL'


@flow(name='Ingest crimes data for month')
def get_crimes_for_month(year: int, month: int) -> pd.DataFrame:
    """Extract row crimes data from URL."""
    logger = get_run_logger()
    logger.info('INFO: Starting downloading data for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))
    if RAW_DATA_CRIMES_URL in os.environ:
        url = os.environ.get(RAW_DATA_CRIMES_URL)
    else:
        url = 'https://data.cityofchicago.org/resource/ijzp-q8t2.csv'

    start_date = '&$where=date{p}20between{p}20{p}27{y}-{m:02d}-01T00:00:00'.format(
        y=year,
        m=month,
        p='%',
    )
    end_date = '{y}-{m:02d}-{d:02d}T23:59:59'.format(
        y=year,
        m=month,
        d=calendar.monthrange(year, month)[1],
    )

    data_filter = '{f}{s}{p}27{p}20and{p}20{p}27{e}{p}27'.format(
        f=start_filter,
        p='%',
        s=start_date,
        e=end_date,
    )

    data_url = '{url}{filter}'.format(
        url=url,
        filter=data_filter,
    )
    logger.info('INFO: Download link {l}'.format(l=data_url))

    df = pd.read_csv(data_url)

    logger.info('INFO: Finishing downloading data for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))
    return df


@task(name='Clean row crimes data')
def clean_crimes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean row crimes data and apply schema."""
    logger = get_run_logger()
    logger.info('INFO: Starting cleaning data')

    df = df[df[[latitude_column]].notnull().all(1)]
    df = df[df[[longitude_column]].notnull().all(1)]
    df.fillna('', inplace=True)
    df = df.astype(schema)

    logger.info('INFO: Finishing cleaning data')

    return df


@task(name='Write crimes data to GCS')
def write_crimes_to_gcs(df: pd.DataFrame, year: int, month: int) -> None:
    """Write crimes data to GCS bucket in parquet format."""
    logger = get_run_logger()
    logger.info('INFO: Starting upload crimes data to GCS for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))

    if GCS_BUCKET_CRIMES_PATH in os.environ:
        to_path_place = os.environ.get(GCS_BUCKET_CRIMES_PATH)
    else:
        to_path_place = 'data/crimes/'

    if GCS_BUCKET_CRIMES_FILE_NAME in os.environ:
        to_path_file = os.environ.get(GCS_BUCKET_CRIMES_FILE_NAME)
    else:
        to_path_file = 'chicago_crimes_'

    if GCS_BUCKET_BLOCK_NAME in os.environ:
        bucket_block = os.environ.get(GCS_BUCKET_BLOCK_NAME)
    else:
        bucket_block = 'chicago-gcs-bucket'

    to_path = '{p}{f}_{y}_{m:02d}.parquet'.format(
        p=to_path_place,
        f=to_path_file,
        y=year,
        m=month,
    )
    gcs_bucket = GcsBucket.load(bucket_block)
    gcs_bucket.upload_from_dataframe(
        df=df,
        to_path=to_path,
        serialization_format=DataFrameSerializationFormat.PARQUET,
    )
    logger.info('INFO: Upload crimes data to GCS complete for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))


@flow(name='Ingest crimes data for year')
def ingest_crimes_for_year(year: int) -> None:
    """Ingest crimes data, clean and write to GCS bucket in parquet format."""
    logger = get_run_logger()
    month_number = 12
    for month in range(1, month_number + 1):
        logger.info('INFO: Starting ingesting crimes data for {y}-{m:02d}'.format(
            y=year,
            m=month,
        ))
        row_df = get_crimes_for_month(year, month)
        if not row_df.empty:
            clean_df = clean_crimes(row_df)
            write_crimes_to_gcs(clean_df, year, month)
        logger.info('INFO: Ingesting crimes data for {y}-{m:02d} complete'.format(
            y=year,
            m=month,
        ))


@flow(name='Ingest row crimes data')
def extract_crimes(years: list[int] = years_default) -> None:
    """Ingest row crimes data."""
    logger = get_run_logger()
    for year in years:
        logger.info('INFO: Starting ingesting crimes data for {y}'.format(
            y=year,
        ))
        ingest_crimes_for_year(year)
        logger.info('INFO: Ingesting crimes data for {y} complete'.format(
            y=year,
        ))
    logger.info('INFO: Ingesting crimes data for complete')


if __name__ == '__main__':
    years = [2020, 2021, 2022]
    extract_crimes(years)
