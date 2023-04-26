# Analyzing the Relationship Between Street Crime and Schools in Chicago

## Problem:
Street crime is a serious issue in Chicago, with thousands of incidents reported each year. This type of crime can range from thefts to assaults and even homicides. While there are many factors that contribute to street crime in Chicago, including poverty, drug use, and gang activity, one possible factor that has received less attention is the proximity of street crime to schools. 

Analyzing the data can provide valuable insights into the relationship between street crime and schools in Chicago. For example, it may be found that certain types of street crimes, such as thefts and robberies, are more likely to occur near schools. Additionally, schools located in certain areas of the city may be at a higher risk of experiencing street crime.

This information can be used to inform strategies for reducing street crime and improving community safety. Law enforcement agencies may consider increasing patrols in areas where street crimes are more likely to occur near schools. Similarly, school administrators may take steps to improve the safety and security of their students and staff, such as installing additional security cameras or hiring more security personnel.

## Sources of information

Data for the analysis taken from the Chicago Data Portal
1. [Crimes - 2001 to Present](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)
This dataset reflects reported incidents of crime (with the exception of murders where data exists for each victim) that occurred in the City of Chicago from 2001 to present, minus the most recent seven days

2. [Chicago Public Schools - School Locations SY2223](https://data.cityofchicago.org/Education/Chicago-Public-Schools-School-Locations-SY2223/gqgn-ekwj)
Locations of educational units in the Chicago Public School District for school year 2022-2023.

## Tech Stack
I make use of the following technologies:

1. Infrastructure: Terraform
2. Conteinerization: Docker and Docker Compose
3. Data lake: Google Cloud Storage
4. Data warehouse: BigQuery
5. Orchestration: Prefect
6. Data transformation: DBT and DBT cloud
7. Data visualization: Google Looker Studio

In developing it, I used
1. Python
2. [Poetry](https://python-poetry.org/docs/)
3. Linter [wemake-python-styleguide](https://wemake-python-styleguide.readthedocs.io/en/latest/index.html)
4. GoogleSQL for BigQuery.

## Data Pipeline Architecture and workflow

![Data diagram](https://github.com/twistby/dtc-project/blob/main/dtc_project/misc/arch.png)

## Dashbord

[Live demo](https://lookerstudio.google.com/u/0/reporting/d439aec8-e412-43e0-b290-b8aad6d278d4/page/V5xND)

![Data diagram](https://github.com/twistby/dtc-project/blob/main/dtc_project/misc/rel.jpg)

## Reproduce it yourself

##### 1. Repo
Fork this [repo](https://github.com/twistby/dtc-project.git), and clone it to your local environment.


##### 2. Google Cloud

Create a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager)

Configure Identity and Access Management (IAM) for the service account, giving it the following privileges:
- Viewer
- Storage Admin
- Storage Object Admin
- BigQuery Admin


Download the JSON credentials and save it, e.g. to ~/.gc/<credentials>
    Copy JSON credentials to project folder dtc_project/keys

Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)

Let the [environment variable](https://cloud.google.com/docs/authentication/application-default-credentials#GAC) point to your GCP key, authenticate it and refresh the session token

```sh
export GOOGLE_APPLICATION_CREDENTIALS=<path_to_your_credentials>.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
gcloud auth application-default login
```

Check out this [link](https://www.youtube.com/watch?v=Hajwnmj0xfQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12&t=29s) for a video walkthrough.


##### 3. Terraform

Run the following commands to install Terraform - if you are using a different OS please choose the correct version [here](https://developer.hashicorp.com/terraform/downloads) and exchange the download link and zip file name

```sh
sudo apt-get install unzip
cd ~/bin
wget https://releases.hashicorp.com/terraform/1.4.1/terraform_1.4.1_linux_amd64.zip
unzip terraform_1.4.1_linux_amd64.zip
rm terraform_1.4.1_linux_amd64.zip
```
Change the variables.tf file in dtc_project/terraform with your corresponding variables.

To initiate, plan and apply the infrastructure, adjust and run the following Terraform commands
```sh
cd terraform/
terraform init
terraform plan -var="project=<your-gcp-project-id>"
terraform apply -var="project=<your-gcp-project-id>"
```
Type 'yes' when prompted.

##### 4. Prefect
[Register](https://app.prefect.cloud/) prefect cloud account

##### 5. dbt cloud

dbt cloud only allows to use API in paid accounts.
If you have paid dbt cloud account, load this [repo](https://github.com/twistby/chicago-crimes.git), set connection to BigQuery and create job with 'dbt build' command


##### 5. Docker

Install [Docker Desktop](https://docs.docker.com/get-docker/)

Set the values of the variables in the Dockerfile starting with PREFECT_KEY

How to get prefect values:
- Login to prefect cloud.
- Go the this url: https://app.prefect.cloud/my/api-keys
- Press the + next to API KEYS
- Add a name and press Create
- Copy the secret value and the name.

How to get dbt values:
- Login to cloud dbt
- Go to : https://cloud.getdbt.com/settings/profile
- Copy API key.
- On the same page, press Projects in the sidebar.
- The url will change to: cloud.getdbt.com/settings/accounts/YOUR ACCOUNT ID
- Copy account id
- Open the Deploy - Jobs and choose required job
- The url will change to: cloud.getdbt.com/deploy/YOUR ACCOUNT ID/projects/YOUR PROJECT ID/jobs/YOUR JOB ID



> Note that the dbt cloud API is only available for paid accounts.
> In case you don't have one, use dbt core flow to transform data.

run the following Docker commands
```sh
docker build -t <your-container-name> .     
docker run -it <your-container-name>
```

After that, in docker container wil run prefect ORION and the following will be created in prefect cloud

Blocks:
1. BigQuery Warehouse
2. dbt Cloud Credentials
3. dbt Cloud Job
4. GCP Credentials
5. GCS Bucket (for data)
6. GCS Bucket (to store flow files)

Deployments:
1. Deploy flows 
2. Ingest row crimes data/extracting all crimes (for ingest data from 2001 to today)
3. Ingest row crimes data/extracting last crimes    At 12:00 PM every day (scheduled flow for ingesting new crimes data)
4. Ingest row schools data/ extracting schools  At 12:00 AM on day 1 of the month (scheduled flow for ingesting schools data)
5. Load data to BQ/load data to BQ		
6. Transform data with dbt cli/run dbt		
7. Transform data with dbt cloud/run dbt-cloud job

All you need to do is run the following deployments

1. Ingest row crimes data/extracting all crimes
2. Ingest row schools data/ extracting schools
3. Load data to BQ/load data to BQ	

If you don't have paid dbt cloud account run 
- Transform data with dbt cli/run dbt

If you have paid dbt cloud account
- Transform data with dbt cloud/run dbt-cloud job

After that, tables will be created in the BigQuery product dataset for further visualization of the information.