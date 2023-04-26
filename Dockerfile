FROM python:3.11.3-slim-bullseye

ARG PROJECT_ENV

ENV PROJECT_ENV=${PROJECT_ENV} \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.3.2 \
    RAW_DATA_CRIMES_URL=https://data.cityofchicago.org/resource/ijzp-q8t2.csv \
    RAW_DATA_SCHOOLS_URL=https://data.cityofchicago.org/resource/gqgn-ekwj.csv \
    PREFECT_KEY=pnu_prefect_api_key \
    PREFECT_WORKSPACE=prefect_handle/workspace_name \
    GCP_PROJECT_ID=your_project_id \
    GCP_SERVICE_ACCOUNT=your_service_account \
    GCP_SERVICE_ACCOUNT_KEY=your_service_account_ceredential_file.json \
    GCP_CREDENTIAL_BLOCK_NAME=chicago-gcp-credentials \
    GCS_BUCKET_BLOCK_NAME=chicago-gcs-bucket \
    GCS_BUCKET_NAME=dtc-de-chicago \
    GCS_DEV_BUCKET_NAME=dtc-de-chicago-dev \
    GCS_BUCKET_CRIMES_PATH=data/crimes/ \
    GCS_BUCKET_CRIMES_FILE_NAME=chicago_crimes_ \
    GCS_BUCKET_SCHOOLS_PATH=data/ \
    GCS_BUCKET_SCHOOLS_FILE_NAME=chicago_schools \
    BQ_BLOCK_NAME=chicago-warehouse \
    BQ_DATASET_NAME=chicago \
    BQ_PROD_DATASET_NAME=chicago_prod  \
    BQ_CRIMES_TABLE_NAME=crimes \
    BQ_SCHOOLS_TABLE_NAME=schools \
    DBT_CREDENTIAL_BLOCK_NAME=chicago-dbt-credentials \
    DBT_JOB_BLOCK_NAME=chicago-dbt-job \
    DBT_API_KEY=your_dbt_cloud_api \
    DBT_ACCOUNT_ID=your_dbt_cloud_id \
    DBT_JOB_ID=your_dbt_cloud_job

COPY docker_setup.sh .

RUN chmod +x docker_setup.sh

RUN ./docker_setup.sh

RUN mkdir root/.dbt

WORKDIR /code

RUN pip install "poetry==$POETRY_VERSION"

COPY poetry.lock pyproject.toml /code/

RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi --no-root

RUN prefect block register -m prefect_gcp
RUN prefect block register -m prefect_dbt

COPY dtc_project /code

COPY prefect_setup.sh .

RUN chmod +x prefect_setup.sh

ENTRYPOINT ["./prefect_setup.sh"]