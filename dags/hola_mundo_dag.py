from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hola_mundo():
    print("🚀 Hola Mundo desde Airflow")


with DAG(
    dag_id="hola_mundo_dag",
    description="DAG de prueba para validar Airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # ejecución manual
    catchup=False,
    tags=["prueba"],
) as dag:

    tarea_hola = PythonOperator(
        task_id="imprimir_hola_mundo",
        python_callable=hola_mundo,
    )