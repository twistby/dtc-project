"""test."""

import json
import os

from prefect import flow, get_run_logger, task
from prefect_dbt.cloud import DbtCloudCredentials, DbtCloudJob
from prefect_gcp import BigQueryWarehouse, GcpCredentials, GcsBucket

GCP_SERVICE_ACCOUNT_KEY = 'GCP_SERVICE_ACCOUNT_KEY'
GCP_CREDENTIAL_BLOCK_NAME = 'GCP_CREDENTIAL_BLOCK_NAME'
GCP_CREDENTIAL_BLOCK_NAME = 'GCP_CREDENTIAL_BLOCK_NAME'
GCS_BUCKET_NAME = 'GCS_BUCKET_NAME'
GCS_DEV_BUCKET_NAME = 'GCS_DEV_BUCKET_NAME'
GCS_BUCKET_BLOCK_NAME = 'GCS_BUCKET_BLOCK_NAME'
DBT_API_KEY = 'DBT_API_KEY'
DBT_ACCOUNT_ID = 'DBT_ACCOUNT_ID'
DBT_CREDENTIAL_BLOCK_NAME = 'DBT_CREDENTIAL_BLOCK_NAME'
DBT_CREDENTIAL_BLOCK_NAME = 'DBT_CREDENTIAL_BLOCK_NAME'
DBT_JOB_ID = 'DBT_JOB_ID'
DBT_JOB_BLOCK_NAME = 'DBT_JOB_BLOCK_NAME'
BQ_BLOCK_NAME = 'BQ_BLOCK_NAME'


@task(name='create GCP credentials block')
def create_gcp_credentials_block() -> None:
    """Create GCP credentials block."""
    logger = get_run_logger()
    logger.info('INFO: start ctreating GCP credentials block')

    with open('/code/keys/{key_name}'.format(
        key_name=os.environ[GCP_SERVICE_ACCOUNT_KEY],
    )) as key_file:
        crdentials = dict(json.load(key_file))

    GcpCredentials(
        service_account_info=crdentials,
    ).save(os.environ[GCP_CREDENTIAL_BLOCK_NAME], overwrite=True)

    logger.info('INFO: finished ctreating GCP credentials block')


@task(name='create GCS-bucket block')
def create_gcs_bucket_block() -> None:
    """Create GCS-Bucket block."""
    logger = get_run_logger()
    logger.info('INFO: start ctreating GCS-bucket block')

    gcp_credentials = GcpCredentials.load(
        os.environ[GCP_CREDENTIAL_BLOCK_NAME],
    )
    GcsBucket(
        bucket=os.environ[GCS_BUCKET_NAME],
        gcp_credentials=gcp_credentials,
    ).save(os.environ[GCS_BUCKET_BLOCK_NAME], overwrite=True)

    GcsBucket(
        bucket=os.environ[GCS_DEV_BUCKET_NAME],
        gcp_credentials=gcp_credentials,
    ).save(os.environ[GCS_DEV_BUCKET_NAME], overwrite=True)

    logger.info('INFO: finished ctreating GCS-bucket block')


@task(name='create BQ block')
def create_bq_block() -> None:
    """Create BQ Warehouse block."""
    logger = get_run_logger()
    logger.info('INFO: start ctreating BQ block')

    gcp_credentials = GcpCredentials.load(
        os.environ[GCP_CREDENTIAL_BLOCK_NAME],
    )
    BigQueryWarehouse(
        gcp_credentials=gcp_credentials,
        fetch_size=1,
    ).save(os.environ[BQ_BLOCK_NAME], overwrite=True)
    logger.info('INFO: finished ctreating BQ block')


@task(name='create dbt credentials block')
def create_dbt_credentials_block() -> None:
    """Create dbt credentials block."""
    logger = get_run_logger()
    logger.info('INFO: Start dbt credentials block creating.')

    DbtCloudCredentials(
        api_key=os.environ[DBT_API_KEY],
        account_id=os.environ[DBT_ACCOUNT_ID],
    ).save(os.environ[DBT_CREDENTIAL_BLOCK_NAME], overwrite=True)

    logger.info('INFO: finished ctreating dbt credentials block')


@task(name='create dbt-cloud-job block')
def create_dbt_cloud_job_block() -> None:
    """Create dbt credentials block."""
    logger = get_run_logger()
    logger.info('INFO: Start dbt-cloud-job block creating.')

    dbt_cloud_credentials = DbtCloudCredentials.load(
        os.environ[DBT_CREDENTIAL_BLOCK_NAME],
    )

    DbtCloudJob(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=os.environ[DBT_JOB_ID],
    ).save(os.environ[DBT_JOB_BLOCK_NAME], overwrite=True)

    logger.info('INFO: finished ctreating dbt-cloud-job block')


@flow(name='Deploy blocks')
def deploy_flows() -> None:
    """Deploy blocks."""
    logger = get_run_logger()
    logger.info('INFO: starting creating Prefect blocks')

    create_gcp_credentials_block()
    create_gcs_bucket_block()
    create_bq_block()
    create_dbt_credentials_block()
    create_dbt_cloud_job_block()

    logger.info('INFO: finished creating Prefect blocks')


if __name__ == '__main__':
    deploy_flows()
