# Airflow image for the daily risk report DAG.
#
# Built rather than using apache/airflow directly because the DAG imports
# psycopg 3 and pyarrow, which the base image does not ship. Installing them
# at build time (instead of via _PIP_ADDITIONAL_REQUIREMENTS) keeps container
# startup fast and the dependency set pinned.
FROM apache/airflow:2.10.4-python3.12

COPY docker/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
