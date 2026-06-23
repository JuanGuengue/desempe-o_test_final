🚀 DataMart ETL Pipeline
<div align="center">










Pipeline ETL para procesamiento de transacciones e-commerce utilizando Apache Airflow, Docker y PostgreSQL.
</div>
📋 Tabla de Contenido
Descripción
Arquitectura
Tecnologías
Estructura del Proyecto
Instalación
Pipeline ETL
Modelo de Datos
Validaciones
Autor
🎯 Descripción

Este proyecto implementa un pipeline ETL de extremo a extremo para DataMart S.A.S., consumiendo datos desde Kaggle, aplicando transformaciones de calidad y cargando la información en PostgreSQL mediante Apache Airflow.

Objetivos

✅ Automatizar la extracción de datos

✅ Estandarizar información transaccional

✅ Persistir información en PostgreSQL

✅ Garantizar idempotencia

✅ Orquestar procesos mediante Airflow

🏛 Arquitectura
                    Kaggle Dataset
                           │
                           ▼
                    Apache Airflow
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
      Extract         Transform            Load
        │                  │                  │
        ▼                  ▼                  ▼
   Raw JSON         Clean Dataset       PostgreSQL
🧰 Tecnologías
Tecnología	Uso
🐍 Python	ETL
🌪 Apache Airflow	Orquestación
🐳 Docker	Contenerización
🐘 PostgreSQL	Data Warehouse
📊 Pandas	Transformación
📦 Kaggle API	Fuente de datos
📂 Estructura
.
├── dags/
│   └── etl_ecommerce_dag.py
│
├── data/
│   ├── raw/
│   └── clean/
│
├── logs/
├── plugins/
│
├── Dockerfile
├── docker-compose.yml
├── README.md
└── .env.example
⚙️ Instalación
1️⃣ Clonar repositorio
git clone <repo>
cd datamart-etl
2️⃣ Crear variables
cp .env.example .env
3️⃣ Construir imágenes
docker compose build
4️⃣ Levantar servicios
docker compose up -d
5️⃣ Verificar servicios
docker compose ps
🔄 Pipeline ETL

GitHub renderiza automáticamente los diagramas Mermaid. 🔥

📊 Reglas de Transformación
❌ Eliminación de cancelaciones.
❌ Eliminación de CustomerID nulos.
❌ Eliminación de cantidades negativas.
❌ Eliminación de precios inválidos.
✅ Normalización de nombres.
✅ Cálculo de revenue.
✅ Segmentación comercial.
🗄 Tabla Destino
ecommerce_transacciones
Campo	Tipo
invoice_no	VARCHAR
stock_code	VARCHAR
quantity	INTEGER
unit_price	NUMERIC
total_venta	NUMERIC
customer_id	VARCHAR
country	VARCHAR
📈 Resultados

El pipeline permite:

Identificar productos más vendidos.
Analizar revenue por país.
Analizar comportamiento de clientes.
Construir reportes analíticos.
Automatizar procesos ETL.
👨‍💻 Autor

Juan Camilo Guengue Pérez

Prueba de Desempeño — Ingeniería de Datos Cohorte 7