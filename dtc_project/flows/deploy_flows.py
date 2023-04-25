"""Deploy flows to prefect."""

import datetime
import os

from extract_crimes_data import extract_crimes
from extract_schools_data import extract_schools
from load_data_to_bq import load_data_to_bq
from prefect import flow, get_run_logger, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_gcp.cloud_storage import GcsBucket
from run_dbt import run_dbt
from run_dbt_cloud import run_dbt_cloud

GCS_DEV_BUCKET_NAME = 'GCS_DEV_BUCKET_NAME'
DBT_CREDENTIAL_BLOCK_NAME = 'DBT_CREDENTIAL_BLOCK_NAME'


@task(name='Deploy deploy flow')
def deploy_deploy_flow() -> None:
    """Deploy flow from this file."""
    logger = get_run_logger()
    logger.info('INFO: Starting deploy flow deployment')

    if GCS_DEV_BUCKET_NAME in os.environ:
        bucket_block = os.environ.get(GCS_DEV_BUCKET_NAME)
    else:
        bucket_block = 'dtc-de-chicago-dev'

    gsc_bucket = GcsBucket.load(bucket_block)

    deployment = Deployment.build_from_flow(
        flow=deploy_flows,
        name='deploy-flows',
        parameters={},
        infra_overrides={'env': {'PREFECT_LOGGING_LEVEL': 'DEBUG'}},
        work_queue_name='default',
        storage=gsc_bucket,
    )

    deployment.apply()
    logger.info('INFO: Deploy flow deployment complete')


@task(name='Deploy all crime extraction flow')
def deploy_extract_crimes(name: str, years: list, schedule: str = '') -> None:
    """Deploy extract crimes flow."""
    logger = get_run_logger()
    logger.info('INFO: Starting deploy extract crimes deployment')

    if GCS_DEV_BUCKET_NAME in os.environ:
        bucket_block = os.environ.get(GCS_DEV_BUCKET_NAME)
    else:
        bucket_block = 'dtc-de-chicago-dev'

    gsc_bucket = GcsBucket.load(bucket_block)

    if schedule == '':
        deployment = Deployment.build_from_flow(
            flow=extract_crimes,
            name=name,
            parameters={'years': years},
            infra_overrides={'env': {'PREFECT_LOGGING_LEVEL': 'DEBUG'}},
            work_queue_name='default',
            storage=gsc_bucket,
        )
    else:
        deployment = Deployment.build_from_flow(
            flow=extract_crimes,
            name=name,
            parameters={'years': years},
            schedule=(CronSchedule(cron=schedule, timezone='UTC')),
            infra_overrides={'env': {'PREFECT_LOGGING_LEVEL': 'DEBUG'}},
            work_queue_name='default',
            storage=gsc_bucket,
        )

    deployment.apply()
    logger.info('INFO: Deploy extract crimes deployment complete')


@task(name='Deploy schools extraction flow')
def deploy_extract_schools() -> None:
    """Deploy extract schools flow."""
    logger = get_run_logger()
    logger.info('INFO: Starting deploy extract schools deployment')

    if GCS_DEV_BUCKET_NAME in os.environ:
        bucket_block = os.environ.get(GCS_DEV_BUCKET_NAME)
    else:
        bucket_block = 'dtc-de-chicago-dev'

    gsc_bucket = GcsBucket.load(bucket_block)

    deployment = Deployment.build_from_flow(
        flow=extract_schools,
        name='extracting schools',
        parameters={},
        schedule=(CronSchedule(cron='0 0 1 * *', timezone='UTC')),
        infra_overrides={'env': {'PREFECT_LOGGING_LEVEL': 'DEBUG'}},
        work_queue_name='default',
        storage=gsc_bucket,
    )

    deployment.apply()
    logger.info('INFO: Deploy extract schools deployment complete')


@task(name='Deploy loading to BQ flow')
def deploy_load_data_to_bq() -> None:
    """Deploy load data to BQ flow."""
    logger = get_run_logger()
    logger.info('INFO: Starting deploy load data to BQ deployment')

    if GCS_DEV_BUCKET_NAME in os.environ:
        bucket_block = os.environ.get(GCS_DEV_BUCKET_NAME)
    else:
        bucket_block = 'dtc-de-chicago-dev'

    gsc_bucket = GcsBucket.load(bucket_block)

    deployment = Deployment.build_from_flow(
        flow=load_data_to_bq,
        name='load data to BQ',
        parameters={},
        infra_overrides={'env': {'PREFECT_LOGGING_LEVEL': 'DEBUG'}},
        work_queue_name='default',
        storage=gsc_bucket,
    )

    deployment.apply()
    logger.info('INFO: Deploy load data to BQ deployment complete')


@task(name='Deploy dbt cloud flow')
def deploy_dbt_cloud_run() -> None:
    """Deploy run dbt-cloud job flow."""
    logger = get_run_logger()
    logger.info('INFO: Starting deploy dbt cloud run deployment')

    if GCS_DEV_BUCKET_NAME in os.environ:
        bucket_block = os.environ.get(GCS_DEV_BUCKET_NAME)
    else:
        bucket_block = 'dtc-de-chicago-dev'

    gsc_bucket = GcsBucket.load(bucket_block)

    deployment = Deployment.build_from_flow(
        flow=run_dbt_cloud,
        name='run dbt-cloud job',
        parameters={},
        infra_overrides={'env': {'PREFECT_LOGGING_LEVEL': 'DEBUG'}},
        work_queue_name='default',
        storage=gsc_bucket,
    )
    deployment.apply()

    logger.info('INFO: Deploy dbt run cloud deployment complete')


@task(name='Deploy dbt flow')
def deploy_dbt_run() -> None:
    """Deploy run dbt-cloud job flow."""
    logger = get_run_logger()
    logger.info('INFO: Starting deploy dbt run deployment')

    if GCS_DEV_BUCKET_NAME in os.environ:
        bucket_block = os.environ.get(GCS_DEV_BUCKET_NAME)
    else:
        bucket_block = 'dtc-de-chicago-dev'

    gsc_bucket = GcsBucket.load(bucket_block)

    deployment = Deployment.build_from_flow(
        flow=run_dbt,
        name='run dbt',
        parameters={},
        infra_overrides={'env': {'PREFECT_LOGGING_LEVEL': 'DEBUG'}},
        work_queue_name='default',
        storage=gsc_bucket,
    )
    deployment.apply()

    logger.info('INFO: Deploy dbt run deployment complete')


@flow(name='Deploy flows')
def deploy_flows() -> None:
    """Deploy flows to prefect."""
    deploy_deploy_flow()

    first_year = 2001
    current_day = datetime.datetime.now()
    current_year = current_day.year

    deploy_extract_crimes(
        name='extracting all crimes',
        years=list(range(first_year, current_year + 1)),
    )
    deploy_extract_crimes(
        name='extracting last crimes',
        years=[current_year],
        schedule='0 12 * * *',
    )

    deploy_extract_schools()
    deploy_load_data_to_bq()
    deploy_dbt_cloud_run()
    deploy_dbt_run()


if __name__ == '__main__':
    deploy_flows()
