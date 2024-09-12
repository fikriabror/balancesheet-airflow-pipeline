# airflow official image
FROM apache/airflow:2.7.0

# install additional packages
RUN pip install pandas openpyxl sqlalchemy psycopg2-binary

# Copy DAGS and other files
COPY ./dags /opt/airflow/dags
COPY ./data /opt/airflow/data
COPY ./logs /opt/airflow/logs
COPY ./plugins /opt/airflow/plugins