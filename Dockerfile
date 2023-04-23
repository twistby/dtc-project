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
    PREFECT_KEY=pnu_hZUXf3mcZmtG3ylmpSWPT0cHIcMDaI1Yiocy \
    PREFECT_WORKSPACE=prefoutlookcom/dtc-project \
    GCP_PROJECT_ID=lithe-vault-375510 \
    GCP_SERVICE_ACCOUNT=lithe-vault-375510-878d76efab6b \
    GCP_SERVICE_ACCOUNT_KEY=lithe-vault-375510-878d76efab6b.json \
    GCP_CREDENTIAL_BLOCK_NAME=DTC-DE-GCP-CREDENTIAL \
    GCS_BUCKET_BLOCK_NAME=DTC-DE-BUCKET-BLOCK \
    GCS_BUCKET_NAME=chicago \
    GCS_BUCKET_CRIMES_PATH=data/crimes/ \
    GCS_BUCKET_CRIMES_FILE_NAME=chicago_crimes_ \
    GCS_BUCKET_SCHOOLS_PATH=data/ \
    GCS_BUCKET_SCHOOLS_FILE_NAME=chicago_schools \
    BQ_BLOCK_NAME=DTC-DE-BQ-WAREHOUSE \
    BQ_DATASET_NAME=chicago \
    BQ_CRIMES_TABEL_NAME=crimes \
    BQ_SCHOOLS_TABEL_NAME=schools \
    DBT_CREDENTIAL_BLOCK_NAME=DTC-DE-DBT-CREDENTIAL \
    DBT_JOB_BLOCK_NAME=DTC-DE-DBT-JOB \
    DBT_API_KEY=cb6371120e359b9814424d4032a3eced7a70be76 \
    DBT_ACCOUNT_ID=148416 \
    DBT_JOB_ID=271407 \
    RAW_DATA_CRIMES_URL=https://data.cityofchicago.org/resource/ijzp-q8t2.csv \
    RAW_DATA_SCHOOLS_URL=https://data.cityofchicago.org/resource/gqgn-ekwj.csv

COPY docker_setup.sh .

RUN chmod +x docker_setup.sh

RUN ./docker_setup.sh

WORKDIR /code

COPY prefect_setup.sh .

RUN chmod +x prefect_setup.sh

RUN pip install "poetry==$POETRY_VERSION"

COPY dtc_project /code

COPY poetry.lock pyproject.toml /code/

RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi --no-root

RUN prefect block register -m prefect_gcp
RUN prefect block register -m prefect_dbt

ENTRYPOINT ["./prefect_setup.sh"]