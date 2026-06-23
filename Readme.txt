🚀 DataMart ETL Pipeline
📌 Descripción

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow, Docker, PostgreSQL y Kaggle como fuente de datos.

El objetivo es demostrar una arquitectura funcional de datos para DataMart S.A.S., permitiendo:

Extraer datos transaccionales desde Kaggle.
Transformar y limpiar la información aplicando reglas de negocio.
Cargar los datos procesados en PostgreSQL/Supabase.
Orquestar todo el proceso mediante Apache Airflow.
Garantizar ejecuciones repetibles e idempotentes.
🏗 Arquitectura
                    Kaggle Dataset
                           │
                           ▼
                  Apache Airflow DAG
                           │
       ┌───────────────────┼───────────────────┐
       │                   │                   │
       ▼                   ▼                   ▼
    Extract             Transform            Load
       │                   │                   │
       ▼                   ▼                   ▼
   Raw JSON          Clean Dataset       PostgreSQL
       │                   │
       ▼                   ▼
 /data/raw         /data/clean
🛠 Tecnologías Utilizadas
Tecnología	Uso
Python 3.12	Desarrollo ETL
Apache Airflow 2.10.5	Orquestación
Docker Compose	Contenerización
PostgreSQL	Repositorio analítico
Supabase	Base de datos destino
Pandas	Transformación de datos
Kaggle API	Obtención de datasets
📂 Estructura del Proyecto
.
├── dags/
│   └── etl_ecommerce_dag.py
│
├── data/
│   ├── raw/
│   └── clean/
│
├── logs/
│
├── plugins/
│
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
📊 Dataset Utilizado

Fuente:

Dataset Ecommerce de Kaggle:

carrie1/ecommerce-data

Contiene:

Facturas
Productos
Clientes
Cantidades
Países
Fechas de compra

Este dataset representa las transacciones operacionales de DataMart.

🔄 Flujo ETL

El DAG implementado se denomina:

etl_ecommerce

y ejecuta las siguientes tareas:

setup
   ↓
extract
   ↓
save_raw
   ↓
transform
   ↓
load
   ↓
save_local
   ↓
notify
1️⃣ Setup

Valida:

Conectividad con PostgreSQL/Supabase.
Existencia de credenciales de Kaggle.
Autenticación con la API de Kaggle.
2️⃣ Extract

Descarga automáticamente el dataset desde Kaggle.

Dataset:

carrie1/ecommerce-data

Los datos son almacenados en:

/opt/airflow/data/raw
3️⃣ Save Raw

Genera una copia JSON de los datos originales para auditoría.

Ejemplo:

ecommerce_raw_2026-06-23_18-20-00.json
4️⃣ Transform

Durante esta etapa se aplican las siguientes reglas:

Eliminación de cancelaciones

Facturas cuyo número inicia con:

C
Eliminación de registros inválidos

Se eliminan:

CustomerID nulos
Quantity <= 0
UnitPrice <= 0
Normalización

Se estandarizan:

StockCode
Description
CustomerID
Country
Conversión de fechas

Se generan:

invoice_date
invoice_hour
Cálculo de métricas
total_venta = quantity * unit_price
Segmentación comercial
Total Venta	Segmento
< 10	Bajo
< 50	Medio
< 200	Alto
>= 200	Premium
5️⃣ Load

Los registros transformados se cargan en:

ecommerce_transacciones

Se utiliza:

ON CONFLICT

para evitar duplicados y garantizar idempotencia.

6️⃣ Save Local

Genera un CSV histórico:

/data/clean/

y actualiza:

ecommerce_latest.csv
7️⃣ Notify

Genera un resumen final:

Total procesados
Insertados
Actualizados
Errores
Archivo generado
Fecha de ejecución
⚙️ Configuración
Variables de Entorno

Crear un archivo:

.env

Ejemplo:

KAGGLE_USERNAME=usuario_kaggle
KAGGLE_KEY=xxxxxxxxxxxxxxxx

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
▶️ Ejecución
1. Clonar repositorio
git clone <repositorio>
cd datamart-etl
2. Construir imágenes
docker compose build
3. Levantar servicios
docker compose up -d
4. Verificar contenedores
docker compose ps

Debe mostrar:

airflow_webserver
airflow_scheduler
airflow_postgres
🌐 Acceso a Airflow
http://localhost:8080

Usuario:

admin

Contraseña:

admin
▶️ Ejecutar el DAG

Dentro de Airflow:

etl_ecommerce
Activar DAG.
Trigger DAG.
Revisar Graph View.
Revisar Logs.
🗄 Tabla Destino
ecommerce_transacciones

Campos principales:

invoice_no
stock_code
description
quantity
unit_price
total_venta
customer_id
country
invoice_date
invoice_hour
segmento_venta
fecha_procesamiento
📈 Decisiones Técnicas
CustomerID nulos

Decisión:

Excluir registros.

Motivo:

No permiten trazabilidad del cliente.

Cancelaciones

Decisión:

Excluir facturas cuyo InvoiceNo inicia con C.

Motivo:

Representan devoluciones o anulaciones.

Duplicados

Decisión:

(invoice_no, stock_code)

se utiliza como clave lógica.

Idempotencia

Se garantiza mediante:

UNIQUE(invoice_no, stock_code)

y

ON CONFLICT DO UPDATE

De esta forma ejecutar el DAG múltiples veces produce el mismo resultado final.

✅ Validaciones

Verificar cantidad de registros:

SELECT COUNT(*)
FROM ecommerce_transacciones;

Top países:

SELECT country,
       COUNT(*) total
FROM ecommerce_transacciones
GROUP BY country
ORDER BY total DESC;

Top ventas:

SELECT stock_code,
       SUM(total_venta) revenue
FROM ecommerce_transacciones
GROUP BY stock_code
ORDER BY revenue DESC
LIMIT 10;
📌 Autor

Juan Camilo Guengue Pérez

Proyecto desarrollado para la Prueba de Desempeño de Ingeniería de Datos – Cohorte 7.