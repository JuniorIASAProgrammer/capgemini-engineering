## How to run airflow dag:

1. Run docker-compose.yaml to set up airflow infrastructure on docker

```
docker-compose up
```

2. Add `airbnb_etl.py` file to a new `dags` folder

3. Make source dataset accessible:
    - add `data` folder inside dag;
    - make `raw` folder and add `.csv` file there;  


```plaintext
# Folder structure of a project

nyc_airbnb_etl_project/
├── dags/
│   ├── nyc_airbnb_etl_dag.py                 # Main Airflow DAG script
│   └── data/
│       ├── raw/
│           └── AB_NYC_2019.csv               # Raw data file
│
├── logs/
│
├── plugins/                                  
│
├── README.md                                 # Project documentation
│
└── requirements.txt                          # Python dependencies for the project
```


4. Open Airflow UI on web and login
```
login: airflow
password: airflow
```

5. Add Postgres connection to airflow Connections (`Admin` -> `Connections`):
```
Connection Id: airflow-airbnb
Host: airflow-postgres-1
Database: airflow_etl
Login: airflow
Password: airflow
Port: 5432
```

6. Open DAGs page and manually run DAG `nyc_aibnb_etl`


<br/><br/>

## DAG Overview

The DAG performs the following tasks:

1. **Data Ingestion (`ingest_data_task`)**:
   - Checks if the raw data file exists in the specified path.
   - Reads the raw data from a CSV file (`AB_NYC_2019.csv`).

2. **Data Transformation (`transform_data_task`)**:
   - Cleans and transforms the raw data by removing invalid values, handling missing data, and converting data types.
   - Saves the transformed data to a new CSV file (`AB_NYC_2019_transformed.csv`).

3. **Create Table in PostgreSQL (`create_listing_table`)**:
   - Creates the target table in the PostgreSQL database if it does not exist.

4. **Load Data to PostgreSQL (`load_data_to_postgres`)**:
   - Loads the transformed data from the CSV file into the PostgreSQL table.
   - Handles SQLAlchemy errors, file errors, and parser errors gracefully with logging.

5. **Data Quality Checks (`quality_check_task`)**:
   - Verifies that the row count in the PostgreSQL table matches the expected count from the transformed data.
   - Checks for NULL values in critical columns.
   - Branches to either `proceed` if checks pass or `log_error` if checks fail.

6. **Proceed (`proceed`)**:
   - Logs a message indicating successful data quality