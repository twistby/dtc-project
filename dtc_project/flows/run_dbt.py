"""Transform data in data warehouse for analitic with dbt-cloud."""

import os

from prefect import flow, get_run_logger, task
from prefect_dbt.cli import DbtCoreOperation

GCP_CREDENTIAL_BLOCK_NAME = 'GCP_CREDENTIAL_BLOCK_NAME'
GCP_PROJECT_ID = 'GCP_PROJECT_ID'
BQ_PROD_DATASET_NAME = 'BQ_PROD_DATASET_NAME'
GCP_SERVICE_ACCOUNT_KEY = 'GCP_SERVICE_ACCOUNT_KEY'


@task(name='Create dbt profile')
def create_dbt_profile() -> None:
    """Create dbt profile."""
    logger = get_run_logger()
    logger.info('INFO: Creating dbt profile')

    gcp_project = os.environ.get(GCP_PROJECT_ID)
    dataset = os.environ.get(BQ_PROD_DATASET_NAME)
    path_to_key = '/code/keys/{key_name}'.format(
        key_name=os.environ[GCP_SERVICE_ACCOUNT_KEY],
    )

    profile_yml = f"""
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: {gcp_project}
      dataset: {dataset}
      location: europe-west6
      threads: 1
      keyfile: '{path_to_key}'
    """  # noqa: WPS221, WPS237, WPS305
    with open('/root/.dbt/profiles.yml', 'w') as profile_file:
        profile_file.write(profile_yml)

    logger.info('INFO: Creating dbt profile complete')


@task(name='Create staging tabels')
def create_staging_tables() -> None:
    """Create staging tabels."""
    logger = get_run_logger()
    logger.info('INFO: Creating staging tables')

    dbt_init = DbtCoreOperation(
        overwrite_profiles=False,
        commands=[
            'dbt deps --project-dir /code/flows',
            'dbt debug --project-dir /code/flows',
            'dbt list --project-dir /code/flows',
            'dbt build --select stg_street_crimes --project-dir /code/flows',
        ],
    )
    dbt_init.run()

    logger.info('INFO: Creating staging tables complete')


@task(name='Create prod tabels')
def create_prod_tables() -> None:
    """Create prod tabels."""
    logger = get_run_logger()
    logger.info('INFO: Creating prod tables')
    dbt_init = DbtCoreOperation(
        overwrite_profiles=False,
        commands=[
            'dbt deps --project-dir /code/flows',
            'dbt debug --project-dir /code/flows',
            'dbt list --project-dir /code/flows',
            'dbt build --select crimes_around_schools --project-dir /code/flows',
        ],
    )
    dbt_init.run()
    logger.info('INFO: Creating prod tables complete')


@flow(name='Transform data with dbt cli')
def run_dbt() -> None:
    """Run dbt cloud job."""
    logger = get_run_logger()
    logger.info('INFO: Starting transform data with dbt cli')

    create_dbt_profile()
    create_staging_tables()
    create_prod_tables()

    logger.info('INFO: Transformating data with dbt complete')


if __name__ == '__main__':
    run_dbt()
