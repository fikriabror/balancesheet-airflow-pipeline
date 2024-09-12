# mekari-project 
# ETL Pipeline Project

## Data Storage and Frameworks

### Data Storage
- **PostgreSQL**: Chosen for its robustness, familiarity, flexibility, and support for complex queries. It also handles large volumes of data efficiently and supports advanced features like indexing and full-text search.

### Frameworks & Libraries
- **Apache Airflow**: Used for orchestrating the ETL process. It provides scheduling, monitoring, and logging capabilities for data workflows.
- **pandas**: Utilized for data manipulation and transformation. Its powerful data structures make it suitable for handling and analyzing data efficiently.
- **Python**: Chosen for its ease of use and extensive libraries that support various data processing tasks.
- **PostgresHook**: Provides seamless integration with PostgreSQL from Airflow, allowing easy execution of SQL queries and data loading.

### Database Schema
- **Tables**: 
  - **Main Table**: `balance_sheet` for raw ingested data.
  - **Fact Table**: `fact_balance_sheet` for calculated data.
- **Schema Design**: Includes auto-incrementing primary keys and handling of conflicts using `ON CONFLICT` statements to ensure data integrity.

### Dataset & Processing
- **Dataset**: given dataset I renamed it becomes dataset.xlsx and store in the folder data. In the container will be copied to opt/airflow/data.
- **Processing**: There are 4 processing here:
    - 1. extract dataset from the airflow container in the data folder and send to xcom to processing data in the next step. Store the data in csv files processing the data using pandas dataframe
    - 2. Transform data consist of standardizing the name of the column, change data type, fill in the null value in the numeric column, replace it with 0, so we can do calculations
    - 3. Load data into postgresql, create table if not exists then insert data using on conflict for update. We named the table balance_sheet
    - 4. fact balance sheet is the calculated table to aggregate all number and using latest balance sheet based on date. load the data into postgresql
    - 5. dag file is etl_dag.py for processing etl using etl_process.py
    - 6. Job run every 2 hours that I implement in etl_dag.py with schedule_interval variable
    - 7. start date I defined now because it's too messy if I run with specific date because it will be run every 2 hours.

### Insights from Resulting Datasets
- Adjustments to data types and removal of unnecessary columns are performed to streamline the dataset.
- Detailed date handling (e.g., using timestamps) facilitates easier sorting and analysis.
- Analysis reveals that higher withdrawal amounts correlate with decreasing balances. Data insights suggest that withdrawals with minimal non-negative balances are less frequent.

## Setup and Running Instructions

### Prerequisites
- Install Python, Docker, and PostgreSQL on your local machine.

### Build Docker Image
docker build -t  .

### Initialize run database Inti
docker-compose run --rm webserver airflow db init

### Docker Compose
docker-compose up -d

### Access Airflow:

- Navigate to http://localhost:8080 in your web browser (default host & port).
- Login to Airflow:

- Use the default credentials:
- Username: admin
- Password: admin
- Set Up PostgreSQL Connection in Airflow (This is for my postgres in my local machine):
- Create a new connection with the following details:
  - Connection ID: local_postgres
  - Host: host.docker.internal
  - Schema: postgres
  - Password: postgres
  - Port: 5432
  - login: postgres

- If you have any problem with the login:
    - Reset the Airflow Password
- If you suspect an issue with the credentials, you can reset the Airflow admin password. Follow these steps:

- Access the Airflow Webserver Container:
docker-compose exec webserver /bin/bash

- airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --role Admin \
  --password newpassword

- Run the DAG:
- Go to the home page in Airflow and trigger the etl_dag to start the ETL process.

- Notes The postgres setup it's working for my local machine. Need to be adjusted based on your local machine

If you have any questions or need further assistance, please feel free to reach out.