"""Transform data in data warehouse for analitic with dbt-cloud."""

import os

from prefect import flow, get_run_logger
from prefect_dbt.cloud import DbtCloudJob
from prefect_dbt.cloud.jobs import run_dbt_cloud_job

DBT_JOB_BLOCK_NAME = 'DBT_JOB_BLOCK_NAME'


@flow(name='Transform data with dbt cloud')
def run_dbt_cloud() -> None:
    """Run dbt cloud job."""
    logger = get_run_logger()
    logger.info('INFO: Starting transform data with dbt cloud')

    dbt_cloud_job = DbtCloudJob.load(os.environ.get(DBT_JOB_BLOCK_NAME))
    run_dbt_cloud_job(dbt_cloud_job=dbt_cloud_job)

    logger.info('INFO: Transformating data with dbt cloud complete')


if __name__ == '__main__':
    run_dbt_cloud()
