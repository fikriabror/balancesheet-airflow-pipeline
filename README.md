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

### Insights from Resulting Datasets
- Adjustments to data types and removal of unnecessary columns are performed to streamline the dataset.
- Detailed date handling (e.g., using timestamps) facilitates easier sorting and analysis.
- Analysis reveals that higher withdrawal amounts correlate with decreasing balances. Data insights suggest that withdrawals with minimal non-negative balances are less frequent.

## Setup and Running Instructions

### Prerequisites
- Install Python, Docker, and PostgreSQL on your local machine.

### Build Docker Image
docker build -t <your-image-name> .

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
  - Username: postgres
- Run the DAG:
- Go to the home page in Airflow and trigger the etl_dag to start the ETL process.

- Notes The postgres setup it's working for my local machine. Need to be adjusted based on your local machine

If you have any questions or need further assistance, please feel free to reach out.