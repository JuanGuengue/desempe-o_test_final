FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir \
    kaggle \
    pandas \
    sqlalchemy \
    psycopg2-binary