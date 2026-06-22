"""
DAG: etl_random_users
Descripción: Extrae 5000 usuarios de la API randomuser.me,
             los transforma con pandas y los carga en PostgreSQL.

Requisitos previos:
  - Conexión en Airflow UI llamada 'postgres_destino'
    (Admin > Connections > postgres_destino)
  - Tabla creada en PostgreSQL (ver SQL al final del archivo)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import logging

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_destino"   # Nombre de tu conexión en Airflow UI
                                        # ⚠️ En Airflow UI, campo Database = 'postgres'
TARGET_DATABASE  = "usuarios_random"    # Base de datos que se crea automáticamente
TARGET_TABLE     = "usuarios_crm"       # Tabla dentro de esa base de datos
LOCAL_DATA_PATH  = "/opt/airflow/data"  # Carpeta montada como volumen en docker-compose
TOTAL_USERS      = 5000
BATCH_SIZE       = 500
RESULTS_PER_CALL = 500

default_args = {
    "owner": "juan_camilo",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# ─────────────────────────────────────────────
# TAREA 0 — SETUP (crea la base de datos)
# ─────────────────────────────────────────────
# def setup(**context):
#     """
#     Se conecta a la base 'postgres' (siempre existe) y crea
#     TARGET_DATABASE si no existe. Debe hacerse con autocommit=True
#     porque CREATE DATABASE no puede correr dentro de una transacción.
#     """
#     hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

#     Obtenemos los datos de conexión para conectarnos manualmente
#     conn_uri = hook.get_uri()  # ej: postgresql://user:pass@host:port/postgres

#     Conectamos directo con psycopg2 para poder usar autocommit
#     conn = psycopg2.connect(conn_uri)
#     conn.autocommit = True      # ← obligatorio para CREATE DATABASE
#     cursor = conn.cursor()

#     Verificar si la base ya existe antes de intentar crearla
#     cursor.execute(
#         "SELECT 1 FROM pg_database WHERE datname = %s",
#         (TARGET_DATABASE,)
#     )
#     existe = cursor.fetchone()

#     if not existe:
#         cursor.execute(f'CREATE DATABASE "{TARGET_DATABASE}"')
#         logging.info(f"✅ Base de datos '{TARGET_DATABASE}' creada exitosamente.")
#     else:
#         logging.info(f"ℹ️  Base de datos '{TARGET_DATABASE}' ya existe, continuando...")

#     cursor.close()
#     conn.close()


# ─────────────────────────────────────────────
# TAREA 1 — EXTRACT
# ─────────────────────────────────────────────
def extract(**context):
    """
    Llama a randomuser.me en batches de 500 hasta completar 5000 usuarios.
    Guarda la lista cruda en XCom.
    """
    logging.info(f"Iniciando extracción de {TOTAL_USERS} usuarios...")
    all_users = []
    calls_needed = TOTAL_USERS // RESULTS_PER_CALL  # 10 llamadas de 500

    for i in range(calls_needed):
        url = f"https://randomuser.me/api/?results={RESULTS_PER_CALL}&seed=corbeta_{i}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        batch = response.json()["results"]
        all_users.extend(batch)
        logging.info(f"  Batch {i+1}/{calls_needed} — {len(batch)} usuarios obtenidos")

    logging.info(f"Extracción completa: {len(all_users)} usuarios en total.")
    context["ti"].xcom_push(key="raw_users", value=all_users)

def save_raw(**context):
    """
    Guarda el JSON crudo de la API en ./data/raw/ antes de transformar.
    """
    import os, json

    raw_users = context["ti"].xcom_pull(key="raw_users", task_ids="extract")

    raw_path = os.path.join(LOCAL_DATA_PATH, "raw")
    os.makedirs(raw_path, exist_ok=True)

    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filepath = os.path.join(raw_path, f"usuarios_raw_{fecha}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(raw_users, f, ensure_ascii=False, indent=2)

    logging.info(f"✅ JSON crudo guardado: {filepath}")
    logging.info(f"   Registros: {len(raw_users)}")


# ─────────────────────────────────────────────
# TAREA 2 — TRANSFORM
# ─────────────────────────────────────────────
def transform(**context):
    """
    Toma los usuarios crudos del XCom, aplica transformaciones con pandas
    y guarda el resultado limpio de vuelta en XCom.
    """
    raw_users = context["ti"].xcom_pull(key="raw_users", task_ids="extract")
    logging.info(f"Transformando {len(raw_users)} usuarios...")

    # Aplanar la estructura anidada del JSON
    records = []
    for u in raw_users:
        records.append({
            "uuid":           u["login"]["uuid"],
            "nombre":         u["name"]["first"],
            "apellido":       u["name"]["last"],
            "genero":         u["gender"],
            "email":          u["email"],
            "telefono":       u["phone"],
            "ciudad":         u["location"]["city"],
            "pais":           u["location"]["country"],
            "codigo_postal":  str(u["location"]["postcode"]),
            "fecha_nacimiento": u["dob"]["date"][:10],   # Solo YYYY-MM-DD
            "edad":           u["dob"]["age"],
            "foto_url":       u["picture"]["large"],
            "fecha_registro_api": u["registered"]["date"][:10],
        })

    df = pd.DataFrame(records)
    logging.info(f"Shape inicial: {df.shape}")

    # ── Transformaciones de calidad ──────────────────────────────────
    # 1. Nombres en Title Case
    df["nombre"]   = df["nombre"].str.strip().str.title()
    df["apellido"] = df["apellido"].str.strip().str.title()

    # 2. Email en minúsculas y validación básica
    df["email"] = df["email"].str.lower().str.strip()
    df = df[df["email"].str.contains(r"^[^@]+@[^@]+\.[^@]+$", na=False)]

    # 3. Eliminar duplicados por uuid
    df = df.drop_duplicates(subset=["uuid"])

    # 4. Eliminar nulos en campos críticos
    df = df.dropna(subset=["uuid", "email", "nombre"])

    # 5. Clasificación por edad
    def segmento_edad(edad):
        if edad < 25:   return "Joven"
        elif edad < 45: return "Adulto"
        elif edad < 65: return "Maduro"
        else:           return "Senior"

    df["segmento_edad"] = df["edad"].apply(segmento_edad)

    # 6. Columna de auditoría
    df["fecha_procesamiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Shape final tras transformaciones: {df.shape}")
    logging.info(f"Segmentos:\n{df['segmento_edad'].value_counts().to_string()}")

    # Pasar a XCom como lista de dicts (JSON serializable)
    context["ti"].xcom_push(key="clean_users", value=df.to_dict(orient="records"))


# ─────────────────────────────────────────────
# TAREA 3 — LOAD
# ─────────────────────────────────────────────
# def load(**context):
#     """
#     Toma los usuarios limpios del XCom y los inserta en PostgreSQL.
#     Crea la tabla automáticamente si no existe.
#     Usa upsert (INSERT ... ON CONFLICT DO UPDATE).
#     """
#     clean_users = context["ti"].xcom_pull(key="clean_users", task_ids="transform")
#     logging.info(f"Cargando {len(clean_users)} usuarios en PostgreSQL...")

#     # Conectarse específicamente a TARGET_DATABASE (no a 'postgres')
#     hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#     conn_uri = hook.get_uri().rsplit("/", 1)[0] + f"/{TARGET_DATABASE}"
#     conn = psycopg2.connect(conn_uri)
#     cursor = conn.cursor()

#     # ── Crear tabla si no existe ─────────────────────────────────────
#     create_table_sql = f"""
#         CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
#             id                   SERIAL PRIMARY KEY,
#             uuid                 VARCHAR(50)  UNIQUE NOT NULL,
#             nombre               VARCHAR(100),
#             apellido             VARCHAR(100),
#             genero               VARCHAR(10),
#             email                VARCHAR(150),
#             telefono             VARCHAR(50),
#             ciudad               VARCHAR(100),
#             pais                 VARCHAR(100),
#             codigo_postal        VARCHAR(20),
#             fecha_nacimiento     DATE,
#             edad                 INTEGER,
#             foto_url             TEXT,
#             fecha_registro_api   DATE,
#             segmento_edad        VARCHAR(20),
#             fecha_procesamiento  TIMESTAMP
#         );
#     """
#     cursor.execute(create_table_sql)
#     conn.commit()
#     logging.info(f"Tabla '{TARGET_TABLE}' lista (creada o ya existía).")

#     insert_sql = f"""
#         INSERT INTO {TARGET_TABLE} (
#             uuid, nombre, apellido, genero, email, telefono,
#             ciudad, pais, codigo_postal, fecha_nacimiento, edad,
#             foto_url, fecha_registro_api, segmento_edad, fecha_procesamiento
#         ) VALUES (
#             %(uuid)s, %(nombre)s, %(apellido)s, %(genero)s, %(email)s, %(telefono)s,
#             %(ciudad)s, %(pais)s, %(codigo_postal)s, %(fecha_nacimiento)s, %(edad)s,
#             %(foto_url)s, %(fecha_registro_api)s, %(segmento_edad)s, %(fecha_procesamiento)s
#         )
#         ON CONFLICT (uuid) DO UPDATE SET
#             nombre              = EXCLUDED.nombre,
#             apellido            = EXCLUDED.apellido,
#             email               = EXCLUDED.email,
#             telefono            = EXCLUDED.telefono,
#             segmento_edad       = EXCLUDED.segmento_edad,
#             fecha_procesamiento = EXCLUDED.fecha_procesamiento;
#     """

#     insertados  = 0
#     actualizados = 0

#     for record in clean_users:
#         cursor.execute(
#             f"SELECT 1 FROM {TARGET_TABLE} WHERE uuid = %(uuid)s",
#             {"uuid": record["uuid"]}
#         )
#         existe = cursor.fetchone()
#         cursor.execute(insert_sql, record)
#         if existe:
#             actualizados += 1
#         else:
#             insertados += 1

#     conn.commit()
#     cursor.close()
#     conn.close()

#     logging.info(f"✅ Carga completa — Insertados: {insertados} | Actualizados: {actualizados}")
#     context["ti"].xcom_push(key="resumen", value={
#         "insertados": insertados,
#         "actualizados": actualizados,
#         "total": len(clean_users)
#     })


# ─────────────────────────────────────────────
# TAREA 4 — SAVE LOCAL
# ─────────────────────────────────────────────
def save_local(**context):
    """
    Guarda los usuarios transformados como CSV en la carpeta local
    montada como volumen Docker (./data en tu máquina).
    Genera un archivo por fecha de ejecución para mantener historial.
    """
    import os

    clean_users = context["ti"].xcom_pull(key="clean_users", task_ids="transform")
    df = pd.DataFrame(clean_users)

    # Crear carpeta si no existe (por si acaso)
    raw_path = os.path.join(LOCAL_DATA_PATH, "clean")
    os.makedirs(raw_path, exist_ok=True)

    # Archivo con fecha para mantener historial de ejecuciones
    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"usuarios_crm_{fecha}.csv"
    filepath = os.path.join(raw_path, filename)

    df.to_csv(filepath, index=False, encoding="utf-8")

    logging.info(f"✅ Archivo guardado localmente: {filepath}")
    logging.info(f"   Registros guardados: {len(df)}")

    # También guardar un archivo 'latest' que siempre se sobreescribe
    latest_path = os.path.join(LOCAL_DATA_PATH, "usuarios_crm_latest.csv")
    df.to_csv(latest_path, index=False, encoding="utf-8")
    logging.info(f"✅ Archivo latest actualizado: {latest_path}")

    context["ti"].xcom_push(key="csv_path", value=filepath)


# ─────────────────────────────────────────────
# TAREA 5 — NOTIFY
# ─────────────────────────────────────────────
def notify(**context):
    """
    Imprime un resumen final del pipeline en los logs de Airflow.
    """
    resumen  = context["ti"].xcom_pull(key="resumen",  task_ids="load")
    csv_path = context["ti"].xcom_pull(key="csv_path", task_ids="save_local")
    logging.info("=" * 50)
    logging.info("📊 RESUMEN DEL PIPELINE ETL — USUARIOS CRM")
    logging.info("=" * 50)
    logging.info(f"  Total procesados : {resumen['total']}")
    logging.info(f"  Insertados       : {resumen['insertados']}")
    logging.info(f"  Actualizados     : {resumen['actualizados']}")
    logging.info(f"  CSV generado     : {csv_path}")
    logging.info(f"  Fecha ejecución  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 50)


# ─────────────────────────────────────────────
# DEFINICIÓN DEL DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="etl_random_users",
    description="ETL de 5000 usuarios desde RandomUser API hacia PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # Todos los días a las 6:00am
    catchup=False,
    tags=["etl", "crm", "usuarios", "corbeta"],
) as dag:

    # t_setup = PythonOperator(
    #     task_id="setup",
    #     python_callable=setup,
    # )

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    t_save_raw = PythonOperator(
        task_id="save_raw",
        python_callable=save_raw,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    # t_load = PythonOperator(
    #     task_id="load",
    #     python_callable=load,
    # )

    t_save_local = PythonOperator(
        task_id="save_local",
        python_callable=save_local,
    )

    t_notify = PythonOperator(
        task_id="notify",
        python_callable=notify,
    )

    # Pipeline: setup >> extract >> transform >> load >> save_local >> notify
    # t_setup >>
    t_extract >> t_save_raw >> t_transform >> t_save_local >> t_notify


# La tabla se crea automáticamente en la tarea 'load' con CREATE TABLE IF NOT EXISTS.