FROM apache/airflow:2.2.3-python3.9

COPY requirements.txt /requirements.txt
# COPY . /swile
# RUN chmod -R 777 /swile

COPY . /opt/airflow/dags/swile
# RUN chmod -R 777 /opt/airflow/dags/swile

# sudo apt-get update && apt-get install postgresql  postgresql-contrib build-essensial libpq-dev -y

# Install additional requirements
RUN pip install --no-cache-dir -r /requirements.txt

# Use the Python script as the entry point
# ENV PYTHONPATH "${PYTHONPATH}:/"
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags"

# RUN chown -R airflow:airflow /opt/airflow/dags
# RUN chmod -R 777 /opt/airflow/dags/swile

# RUN chown -R airflow:airflow /opt/airflow/dags/swile && \
#     chmod -R 777 /opt/airflow/dags/swile


# ENTRYPOINT ["python", "/swile/scripts/prepare_postgresql.py"]
# ENTRYPOINT ["python", "/opt/airflow/dags/swile/scripts/prepare_postgresql.py"]
ENTRYPOINT ["airflow", "standalone"]

# After the script runs, start Airflow in standalone mode
# CMD ["airflow", "standalone"]
# CMD ["scheduler"]
