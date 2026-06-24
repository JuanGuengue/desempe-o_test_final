"""
DAG: etl_ecommerce
Descripción: Descarga el dataset de transacciones ecommerce desde Kaggle (carrie1/ecommerce-data),
             lo transforma con pandas y lo carga en Supabase/PostgreSQL.

Requisitos previos:
  - Conexión en Airflow llamada 'postgres_destino' (variable de entorno AIRFLOW_CONN_POSTGRES_DESTINO)
  - Variables de entorno KAGGLE_USER y KAGGLE_KEY en el docker-compose
  - Librería kaggle instalada en los contenedores
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_destino"
TARGET_TABLE     = "ecommerce_transacciones"
TARGET_TABLE_RAW    = "ecommerce_transacciones_raw"
LOCAL_DATA_PATH  = "/opt/airflow/data"
KAGGLE_DATASET   = "carrie1/ecommerce-data"
CSV_FILENAME     = "data.csv"

default_args = {
    "owner": "juan_camilo",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),      
    "email_on_failure": False,
}

# ─────────────────────────────────────────────
# TAREA 0 — SETUP
# ─────────────────────────────────────────────
def setup(**context):
    """
    Valida la conexión a Supabase y las credenciales de Kaggle.
    """
    # ── Validar Supabase ──────────────────────────────────────────────
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    logging.info("✅ Conexión a Supabase exitosa.")
    logging.info(f"   PostgreSQL version: {version[0]}")
    cursor.close()
    conn.close()

    # ── Validar Kaggle ────────────────────────────────────────────────
    kaggle_user = os.getenv("KAGGLE_USERNAME")
    kaggle_key  = os.getenv("KAGGLE_KEY")

    if not kaggle_user or not kaggle_key:
        raise ValueError(
            "❌ Credenciales de Kaggle no encontradas. "
            "Verifica KAGGLE_USER y KAGGLE_KEY en el docker-compose."
        )

    os.environ["KAGGLE_USERNAME"] = kaggle_user
    os.environ["KAGGLE_KEY"]      = kaggle_key
    import kaggle
    api = kaggle.api
    api.authenticate()
    logging.info(f"✅ Kaggle autenticado correctamente. Usuario: {kaggle_user}")


# ─────────────────────────────────────────────
# TAREA 1 — EXTRACT
# ─────────────────────────────────────────────
def extract(**context):
    """
    Descarga el dataset ecommerce desde Kaggle y lo guarda en /data/raw/.
    Empuja los registros crudos a XCom.
    """
    os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
    os.environ["KAGGLE_KEY"]      = os.getenv("KAGGLE_KEY")

    import kaggle
    api = kaggle.api
    api.authenticate()

    download_path = os.path.join(LOCAL_DATA_PATH, "raw")
    os.makedirs(download_path, exist_ok=True)

    logging.info(f"Descargando dataset: {KAGGLE_DATASET}")
    api.dataset_download_files(
        KAGGLE_DATASET,
        path=download_path,
        unzip=True
    )
    logging.info(f"✅ Dataset descargado en {download_path}")

    filepath = os.path.join(download_path, CSV_FILENAME)
    df = pd.read_csv(filepath, encoding="latin-1")

    logging.info(f"Registros leídos: {len(df)}")
    logging.info(f"Columnas: {list(df.columns)}")

    context["ti"].xcom_push(key="raw_data", value=df.to_dict(orient="records"))


# ─────────────────────────────────────────────
# TAREA 2 — SAVE RAW
# ─────────────────────────────────────────────
def save_raw(**context):
    """
    Persiste una copia del JSON crudo en /data/raw/ con timestamp.
    """
    import json

    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")

    raw_path = os.path.join(LOCAL_DATA_PATH, "raw")
    os.makedirs(raw_path, exist_ok=True)

    fecha    = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filepath = os.path.join(raw_path, f"ecommerce_raw_{fecha}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=2)

    logging.info(f"✅ JSON crudo guardado: {filepath}")
    logging.info(f"   Registros: {len(raw_data)}")


# ─────────────────────────────────────────────
# TAREA 3 — TRANSFORM
# ─────────────────────────────────────────────
def transform(**context):
    """
    Limpia y transforma el dataset ecommerce:
      - Elimina cancelaciones (InvoiceNo que empieza con 'C')
      - Elimina filas sin CustomerID
      - Normaliza tipos de datos
      - Calcula total_venta = Quantity * UnitPrice
      - Clasifica transacciones por valor
      - Agrega columna de auditoría
    """
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    df = pd.DataFrame(raw_data)
    logging.info(f"Shape inicial: {df.shape}")

    # 1. Eliminar cancelaciones
    df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]
    logging.info(f"Tras eliminar cancelaciones: {df.shape}")

    # 2. Eliminar filas sin CustomerID
    df = df.dropna(subset=["CustomerID"])
    logging.info(f"Tras eliminar sin CustomerID: {df.shape}")

    # 3. Eliminar filas con Quantity o UnitPrice negativos o cero
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]
    logging.info(f"Tras filtrar cantidades/precios inválidos: {df.shape}")

    # 4. Eliminar duplicados
    df = df.drop_duplicates()

    # 5. Normalizar tipos
    df["InvoiceNo"]   = df["InvoiceNo"].astype(str).str.strip()
    df["StockCode"]   = df["StockCode"].astype(str).str.strip()
    df["Description"] = df["Description"].astype(str).str.strip().str.title()
    df["CustomerID"]  = df["CustomerID"].astype(int).astype(str)
    df["Country"]     = df["Country"].astype(str).str.strip()
    df["Quantity"]    = df["Quantity"].astype(int)
    df["UnitPrice"]   = df["UnitPrice"].astype(float).round(2)

    # 6. Parsear fecha
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], format="mixed", dayfirst=False)
    df["invoice_date"] = df["InvoiceDate"].dt.strftime("%Y-%m-%d")
    df["invoice_hour"] = df["InvoiceDate"].dt.hour

    # 7. Calcular total de venta por línea
    df["total_venta"] = (df["Quantity"] * df["UnitPrice"]).round(2)

    # 8. Clasificar transacción por valor
    def segmento_venta(total):
        if total < 10:    return "Bajo"
        elif total < 50:  return "Medio"
        elif total < 200: return "Alto"
        else:             return "Premium"

    df["segmento_venta"] = df["total_venta"].apply(segmento_venta)

    # 9. Renombrar columnas a snake_case
    df = df.rename(columns={
        "InvoiceNo":   "invoice_no",
        "StockCode":   "stock_code",
        "Description": "description",
        "Quantity":    "quantity",
        "UnitPrice":   "unit_price",
        "CustomerID":  "customer_id",
        "Country":     "country",
    })

    # 10. Seleccionar columnas finales
    df = df[[
        "invoice_no", "stock_code", "description", "quantity",
        "unit_price", "total_venta", "customer_id", "country",
        "invoice_date", "invoice_hour", "segmento_venta"
    ]]
    df = df.drop_duplicates(subset=["invoice_no", "stock_code"], keep="last")
    logging.info(f"Shape tras deduplicar: {df.shape}")

    # 11. Columna de auditoría
    df["fecha_procesamiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Shape final: {df.shape}")
    logging.info(f"Segmentos de venta:\n{df['segmento_venta'].value_counts().to_string()}")
    logging.info(f"Países únicos: {df['country'].nunique()}")

    context["ti"].xcom_push(key="clean_data", value=df.to_dict(orient="records"))


# ─────────────────────────────────────────────
# TAREA 3B — LOAD RAW (Bronze)
# ─────────────────────────────────────────────
def load_raw(**context):
    from psycopg2.extras import execute_values

    raw_path = os.path.join(LOCAL_DATA_PATH, "raw", CSV_FILENAME)
    df = pd.read_csv(raw_path, encoding="latin-1", dtype=str)
    df = df.where(pd.notnull(df), None)

    logging.info(f"Registros crudos a cargar: {len(df)}")

    hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    # Crear tabla si no existe
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_RAW} (
            id            SERIAL PRIMARY KEY,
            invoice_no    TEXT,
            stock_code    TEXT,
            description   TEXT,
            quantity      TEXT,
            invoice_date  TEXT,
            unit_price    TEXT,
            customer_id   TEXT,
            country       TEXT,
            fecha_ingesta TIMESTAMP DEFAULT NOW()
        );
    """)

    # Limpiar antes de insertar — evita acumulación en reintento
    cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE_RAW} RESTART IDENTITY;")
    conn.commit()
    logging.info("Tabla Bronze limpiada, insertando datos frescos...")

    BATCH_SIZE = 5000   
    total      = len(df)
    insertados = 0

    for i in range(0, total, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE]

        valores = [
            (
                r.get("InvoiceNo"),  r.get("StockCode"), r.get("Description"),
                r.get("Quantity"),   r.get("InvoiceDate"), r.get("UnitPrice"),
                r.get("CustomerID"), r.get("Country")
            )
            for _, r in batch.iterrows()
        ]

        execute_values(cursor, f"""
            INSERT INTO {TARGET_TABLE_RAW} (
                invoice_no, stock_code, description, quantity,
                invoice_date, unit_price, customer_id, country
            ) VALUES %s;
        """, valores)

        conn.commit()
        insertados += len(batch)
        logging.info(f"  Bronze — Lote {i//BATCH_SIZE + 1} — {insertados}/{total}")

    cursor.close()
    conn.close()
    logging.info(f"✅ Bronze cargado — {insertados} registros en '{TARGET_TABLE_RAW}'")

# ─────────────────────────────────────────────
# TAREA 4 — LOAD
# ─────────────────────────────────────────────
def load(**context):
    clean_data = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    logging.info(f"Cargando {len(clean_data)} registros en Supabase...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()

    # ── Crear tabla si no existe ──────────────────────────────────────
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id                  SERIAL PRIMARY KEY,
            invoice_no          VARCHAR(20)  NOT NULL,
            stock_code          VARCHAR(20)  NOT NULL,
            description         TEXT,
            quantity            INTEGER,
            unit_price          NUMERIC(10,2),
            total_venta         NUMERIC(10,2),
            customer_id         VARCHAR(20),
            country             VARCHAR(100),
            invoice_date        DATE,
            invoice_hour        INTEGER,
            segmento_venta      VARCHAR(20),
            fecha_procesamiento TIMESTAMP,
            UNIQUE (invoice_no, stock_code)
        );
    """)
    conn.commit()
    logging.info(f"Tabla '{TARGET_TABLE}' lista.")

    # ── Inserción en lotes con execute_values ─────────────────────────
    from psycopg2.extras import execute_values

    BATCH_SIZE = 1000
    df = pd.DataFrame(clean_data)
    total = len(df)
    insertados = 0

    for i in range(0, total, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE]

        valores = [
            (
                r["invoice_no"], r["stock_code"], r["description"], r["quantity"],
                r["unit_price"], r["total_venta"], r["customer_id"], r["country"],
                r["invoice_date"], r["invoice_hour"], r["segmento_venta"], r["fecha_procesamiento"]
            )
            for _, r in batch.iterrows()
        ]

        execute_values(cursor, f"""
            INSERT INTO {TARGET_TABLE} (
                invoice_no, stock_code, description, quantity,
                unit_price, total_venta, customer_id, country,
                invoice_date, invoice_hour, segmento_venta, fecha_procesamiento
            ) VALUES %s
            ON CONFLICT (invoice_no, stock_code) DO UPDATE SET
                description         = EXCLUDED.description,
                quantity            = EXCLUDED.quantity,
                unit_price          = EXCLUDED.unit_price,
                total_venta         = EXCLUDED.total_venta,
                segmento_venta      = EXCLUDED.segmento_venta,
                fecha_procesamiento = EXCLUDED.fecha_procesamiento;
        """, valores)

        conn.commit()
        insertados += len(batch)
        logging.info(f"  Lote {i//BATCH_SIZE + 1} — {insertados}/{total} registros insertados")

    cursor.close()
    conn.close()

    logging.info(f"✅ Carga completa — {insertados} registros en {total//BATCH_SIZE + 1} lotes")
    context["ti"].xcom_push(key="resumen", value={
        "insertados":   insertados,
        "actualizados": 0,
        "errores":      0,
        "total":        total
    })
# ─────────────────────────────────────────────
# TAREA 5 — SAVE LOCAL
# ─────────────────────────────────────────────
def save_local(**context):
    """
    Guarda el CSV transformado en /data/clean/ con timestamp e historial.
    """
    clean_data = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    df = pd.DataFrame(clean_data)

    clean_path = os.path.join(LOCAL_DATA_PATH, "clean")
    os.makedirs(clean_path, exist_ok=True)

    fecha    = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"ecommerce_clean_{fecha}.csv"
    filepath = os.path.join(clean_path, filename)
    df.to_csv(filepath, index=False, encoding="utf-8")
    logging.info(f"✅ Archivo guardado: {filepath} ({len(df)} registros)")

    latest_path = os.path.join(LOCAL_DATA_PATH, "ecommerce_latest.csv")
    df.to_csv(latest_path, index=False, encoding="utf-8")
    logging.info(f"✅ Archivo latest actualizado: {latest_path}")

    context["ti"].xcom_push(key="csv_path", value=filepath)


# ─────────────────────────────────────────────
# TAREA 6 — NOTIFY
# ─────────────────────────────────────────────
def notify(**context):
    """
    Imprime resumen final del pipeline en los logs de Airflow.
    """
    resumen  = context["ti"].xcom_pull(key="resumen",  task_ids="load")
    csv_path = context["ti"].xcom_pull(key="csv_path", task_ids="save_local")

    logging.info("=" * 55)
    logging.info("📊 RESUMEN DEL PIPELINE ETL — ECOMMERCE TRANSACCIONES")
    logging.info("=" * 55)
    logging.info(f"  Total procesados : {resumen['total']}")
    logging.info(f"  Insertados       : {resumen['insertados']}")
    logging.info(f"  Actualizados     : {resumen['actualizados']}")
    logging.info(f"  Errores          : {resumen['errores']}")
    logging.info(f"  CSV generado     : {csv_path}")
    logging.info(f"  Fecha ejecución  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 55)


# ─────────────────────────────────────────────
# DEFINICIÓN DEL DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="etl_ecommerce",
    description="ETL de transacciones ecommerce desde Kaggle hacia Supabase/PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["etl", "ecommerce", "kaggle", "corbeta"],
) as dag:

    t_setup = PythonOperator(
        task_id="setup",
        python_callable=setup,
    )
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
    t_load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw,
    )
    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )
    t_save_local = PythonOperator(
        task_id="save_local",
        python_callable=save_local,
    )
    t_notify = PythonOperator(
        task_id="notify",
        python_callable=notify,
    )

    t_setup >> t_extract >> t_save_raw >> t_transform >> t_load_raw >> t_load >> t_save_local >> t_notify
