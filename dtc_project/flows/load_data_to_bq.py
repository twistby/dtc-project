"""Loading data from datalake and to datawarehouse using Prefect BQ block."""

import os

from prefect import flow, get_run_logger, task
from prefect_gcp.bigquery import BigQueryWarehouse

GCP_PROJECT_ID = 'GCP_PROJECT'

BQ_BLOCK_NAME = 'BQ_BLOCK_NAME'
BQ_DATASET_NAME = 'BQ_DATASET_NAME'
BQ_CRIMES_TABEL_NAME = 'BQ_CRIMES_TABEL_NAME'
BQ_SCHOOLS_TABEL_NAME = 'BQ_SCHOOLS_TABEL_NAME'

GCS_BUCKET_NAME = 'GCS_BUCKET_NAME'
GCS_BUCKET_CRIMES_PATH = 'GCS_BUCKET_CRIMES_PATH'
GCS_BUCKET_CRIMES_FILE_NAME = 'GCS_BUCKET_CRIMES_FILE_NAME'
GCS_BUCKET_SCHOOLS_PATH = 'GCS_BUCKET_SCHOOLS_PATH'
GCS_BUCKET_SCHOOLS_FILE_NAME = 'GCS_BUCKET_SCHOOLS_FILE_NAME'

if GCP_PROJECT_ID in os.environ:
    gcp_project_name = os.environ.get(GCP_PROJECT_ID)
else:
    gcp_project_name = ''

if BQ_BLOCK_NAME in os.environ:
    bq_block_name = os.environ.get(BQ_BLOCK_NAME)
else:
    bq_block_name = 'DTC-DE-BQ-WAREHOUSE'

if BQ_DATASET_NAME in os.environ:
    bq_dataset_name = os.environ.get(BQ_DATASET_NAME)
else:
    bq_dataset_name = 'chicago'

if BQ_CRIMES_TABEL_NAME in os.environ:
    bq_crimes_table_name = os.environ.get(BQ_CRIMES_TABEL_NAME)
else:
    bq_crimes_table_name = 'crimes'

if BQ_SCHOOLS_TABEL_NAME in os.environ:
    bq_schools_table_name = os.environ.get(BQ_SCHOOLS_TABEL_NAME)
else:
    bq_schools_table_name = 'schools'


if GCS_BUCKET_NAME in os.environ:
    bucket_name = os.environ.get(GCS_BUCKET_NAME)
else:
    bucket_name = 'chicago'

if GCS_BUCKET_CRIMES_PATH in os.environ:
    from_path_crimes = os.environ.get(GCS_BUCKET_CRIMES_PATH)
else:
    from_path_crimes = 'data/crimes/'

if GCS_BUCKET_CRIMES_FILE_NAME in os.environ:
    crimes_file_name = os.environ.get(GCS_BUCKET_CRIMES_FILE_NAME)
else:
    crimes_file_name = 'chicago_crimes_'


if GCS_BUCKET_SCHOOLS_PATH in os.environ:
    from_path_schools = os.environ.get(GCS_BUCKET_SCHOOLS_PATH)
else:
    from_path_schools = 'data/'

if GCS_BUCKET_SCHOOLS_FILE_NAME in os.environ:
    schools_file_name = os.environ.get(GCS_BUCKET_SCHOOLS_FILE_NAME)
else:
    schools_file_name = 'schools'


@task(name='create external crimes table')
def create_ext_crimes_table() -> None:
    """Create external crimes table from files in datalake."""
    logger = get_run_logger()
    logger.info('INFO: Starting load crimes to external table')
    with BigQueryWarehouse.load(bq_block_name) as warehouse:
        operation = """
        CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.external_{table}`
        OPTIONS (
            format = 'PARQUET',
            uris = ['gs://{bucket}/{from_path}{file}*.parquet']
        );
        """.format(  # noqa: WPS462
            project=gcp_project_name,
            dataset=bq_dataset_name,
            table=bq_crimes_table_name,
            bucket=bucket_name,
            from_path=from_path_crimes,
            file=crimes_file_name,
        )
        warehouse.execute(operation)
    logger.info('INFO: Loadig crimes to external table complete')


@task(name='create partitioned and clustered crimes table')
def create_crimes_table() -> None:
    """Create partitioned and clustered crimes table from external table."""
    logger = get_run_logger()
    logger.info('INFO: Starting load crimes to partitioned table')
    with BigQueryWarehouse.load(bq_block_name) as warehouse:
        operation = """
        CREATE OR REPLACE TABLE `{project}.{dataset}.{table}`
        PARTITION BY DATE(date)
        CLUSTER BY location_description AS
        SELECT * FROM `{project}.{dataset}.external_{table}`;
        """.format(  # noqa: WPS462
            project=gcp_project_name,
            dataset=bq_dataset_name,
            table=bq_crimes_table_name,
        )
        warehouse.execute(operation)
    logger.info('INFO: Loadig crimes to partitioned table complete')


@task(name='create external shools table')
def create_ext_schools_table() -> None:
    """Create external schools table from files in datalake."""
    logger = get_run_logger()
    logger.info('INFO: Starting load schools to external table')
    with BigQueryWarehouse.load(bq_block_name) as warehouse:
        operation = """
        CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.external_{table}`
        OPTIONS (
            format = 'PARQUET',
            uris = ['gs://{bucket}/{from_path}{file}*.parquet']
        );
        """.format(  # noqa: WPS462
            project=gcp_project_name,
            dataset=bq_dataset_name,
            table=bq_schools_table_name,
            bucket=bucket_name,
            from_path=from_path_schools,
            file=schools_file_name,
        )
        warehouse.execute(operation)
    logger.info('INFO: Loadig schools schools to external table complete')


@task(name='create shools table')
def create_schools_table() -> None:
    """Create schools table from external table."""
    logger = get_run_logger()
    logger.info('INFO: Starting load schools to table')
    with BigQueryWarehouse.load(bq_block_name) as warehouse:
        operation = """
        CREATE OR REPLACE TABLE `{project}.{dataset}.{table}` AS
        SELECT * FROM `{project}.{dataset}.external_{table}`;
        """.format(  # noqa: WPS462
            project=gcp_project_name,
            dataset=bq_dataset_name,
            table=bq_schools_table_name,
        )
        warehouse.execute(operation)
    logger.info('INFO: Loadig schools schools to table complete')


@flow(name='Load data to BQ')
def load_data_to_bq() -> None:
    """Load crimes and schols data to bq."""
    logger = get_run_logger()
    logger.info('INFO: Starting loadig data to BQ')
    create_ext_crimes_table()
    create_crimes_table()
    create_ext_schools_table()
    create_schools_table()
    logger.info('INFO: Loadig data to BQ complete')


if __name__ == '__main__':
    load_data_to_bq()
