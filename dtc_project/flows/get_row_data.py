"""Getting row data from Chicago data portal."""
import os

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect_gcp.cloud_storage import DataFrameSerializationFormat, GcsBucket

latitude_column = 'latitude'
longitude_column = 'longitude'
years_default = [2022, 2023]
crimes_schema = {  # noqa: WPS417
    'id': 'int64',
    'case_number': 'string',
    'date': 'datetime64',
    'block': 'string',
    'iucr': 'string',
    'primary_type': 'string',
    'description': 'string',
    'location_description': 'string',
    'arrest': 'bool',
    'domestic': 'bool',
    'beat': 'string',
    'district': 'string',
    'id': 'int64',
    'ward': 'int64',
    'community_area': 'string',
    'fbi_code': 'string',
    'x_coordinate': 'float64',
    'y_coordinate': 'float64',
    'year': 'int64',
    'updated_on': 'datetime64',
    'latitude': 'float64',
    'longitude': 'float64',
    'location': 'string',
}


@flow(name='Ingest data for month')
def get_data_for_month(year: int, month: int) -> pd.DataFrame:
    """Extract row data from URL."""
    logger = get_run_logger()
    logger.info('INFO: Starting downloading data for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))
    if 'RAW_DATA_CRIMES_URL' in os.environ():
        url = os.environ('RAW_DATA_CRIMES_URL')
    else:
        url = 'https://data.cityofchicago.org/resource/ijzp-q8t2.csv'

    start_filter = '?$where=date{p}20between{p}'.format(p='%')
    start_date = '{y}-{m:02d}-01T00:00:01'.format(y=year, m=month)
    end_date = '{y}-{m:02d}-31T23:59:59'.format(y=year, m=month)

    fstring = '{f}20{p}27{s}{p}27{p}20and{p}20{p}27{e}{p}27'.format(
        f=start_filter,
        p='%',
        s=start_date,
        e=end_date,
    )

    data_url = '{url}{filter}'.format(
        url=url,
        filter=fstring,
    )
    logger.info('INFO: Download link {l}'.format(l=data_url))

    df = pd.read_csv(data_url)

    logger.info('INFO: Finishing downloading data for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))
    return df


@task(name='Clean row data')
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean row data and apply schema."""
    logger = get_run_logger()
    logger.info('INFO: Starting cleaning data')

    df = df[df[[latitude_column]].notnull().all(1)]
    df = df[df[[longitude_column]].notnull().all(1)]
    df = df.astype(crimes_schema)

    logger.info('INFO: Finishing cleaning data')

    return df


@task(name='Write data to GCS')
def write_to_gcs(df: pd.DataFrame, year: int, month: int) -> None:
    """Write data to GSC bucket in parquet format."""
    logger = get_run_logger()
    logger.info('INFO: Starting upload data to GSC for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))
    to_path = 'data/crimes/{y}/chicago_crimes_{y}_{m:02d}.parquet'.format(
        y=year,
        m=month,
    )
    gcs_bucket = GcsBucket.load(os.environ('GCS_BUCKET_BLOCK_NAME'))
    gcs_bucket.upload_from_dataframe(
        df=df,
        to_path=to_path,
        serialization_format=DataFrameSerializationFormat.PARQUET,
    )
    logger.info('INFO: Uploading data to GSC complete for {y}-{m:02d}'.format(
        y=year,
        m=month,
    ))


@flow(name='Ingest data for year')
def ingest_data_for_year(year: int) -> None:
    """Ingest data, clean and write to GSC bucket in parquet format."""
    logger = get_run_logger()
    month_number = 12
    for month in range(1, month_number + 1):
        logger.info('INFO: Starting ingesting data for {y}-{m:02d}'.format(
            y=year,
            m=month,
        ))
        row_df = get_data_for_month(year, month)
        df = clean_data(row_df)
        write_to_gcs(df, year, month)
        logger.info('INFO: Ingesting data for {y}-{m:02d} complete'.format(
            y=year,
            m=month,
        ))


@flow(name='Ingest row data to GCS Bucket')
def ingest_data(years: list[int] = years_default) -> None:
    """Ingest row data to GCS-bucket."""
    logger = get_run_logger()
    for year in years:
        logger.info('INFO: Starting ingesting data for {y}'.format(
            y=year,
        ))
        ingest_data_for_year(year)
        logger.info('INFO: Ingesting data for {y} complete'.format(
            y=year,
        ))
    logger.info('INFO: Ingesting data for complete')


if __name__ == '__main__':
    years = [2020, 2021, 2022]
    ingest_data(years)
