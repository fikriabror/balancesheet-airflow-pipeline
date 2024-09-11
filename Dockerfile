# airflow official image
FROM apache/airflow:2.7.0

# Switch the user for installation
USER root

# install additional packages
RUN pip install pandas openpyxl sqlalchemy psycopg2-binary

# switch back to airflow user
USER airflow

# Copy DAGS and other files
COPY ./dags /opt/airflow/dags
COPY ./data /opt/airflow/data
COPY ./logs /opt/airflow/logs
COPY ./plugins /opt/airflow/plugins

# Entrypoint
ENTRYPOINT [ "/usr/local/bin/entrypoint" ]
CMD [ "webserver" ]