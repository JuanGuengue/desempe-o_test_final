# 🚀 DataMart ETL Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.5-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-blue)
![Docker](https://img.shields.io/badge/Docker-2496ED)
![Kaggle](https://img.shields.io/badge/Kaggle-Dataset-20BEFF)

### End-to-End ETL Pipeline for E-commerce Transaction Processing using Apache Airflow, Docker, PostgreSQL, and Kaggle.

</div>

---

# 📖 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Project Structure](#-project-structure)
- [Installation & Setup](#-installation--setup)
- [ETL Workflow](#-etl-workflow)
- [Data Transformation Rules](#-data-transformation-rules)
- [Target Data Model](#-target-data-model)
- [Business Insights](#-business-insights)
- [Author](#-author)

---

# 🎯 Project Overview

This project implements a complete **ETL (Extract, Transform, Load) pipeline** designed for DataMart S.A.S.

The solution automatically extracts transactional data from Kaggle, applies business and data quality transformations, and loads the processed information into PostgreSQL.

## Key Objectives

✅ Automate data extraction

✅ Standardize transactional records

✅ Store processed data in PostgreSQL

✅ Ensure idempotent executions

✅ Orchestrate workflows using Apache Airflow

✅ Enable analytical reporting and business intelligence

---

# 🏛 Architecture

```text
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
      Raw Data       Clean Dataset      PostgreSQL
```

---

# 🧰 Technology Stack

| Technology | Purpose |
|------------|----------|
| 🐍 Python | ETL Development |
| 🌪 Apache Airflow | Workflow Orchestration |
| 🐳 Docker | Containerization |
| 🐘 PostgreSQL | Data Storage |
| 📊 Pandas | Data Transformation |
| 📦 Kaggle API | Data Source |

---

# 📂 Project Structure

```bash
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
├── README.md
└── .env.example
```

---

# ⚙️ Installation & Setup

## 1. Clone the Repository

```bash
git clone <repository-url>
cd datamart-etl
```

## 2. Configure Environment Variables

```bash
cp .env.example .env
```

Update the `.env` file with your credentials:

```env
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key
```

---

## 3. Build Docker Images

```bash
docker compose build
```

---

## 4. Start Services

```bash
docker compose up -d
```

---

## 5. Verify Running Containers

```bash
docker compose ps
```

Expected services:

- airflow_webserver
- airflow_scheduler
- airflow_postgres

---

# 🌐 Access Airflow

Open:

```text
http://localhost:8080
```

Default credentials:

```text
Username: admin
Password: admin
```

---

# 🔄 ETL Workflow

The ETL process follows the sequence below:

```mermaid
flowchart LR

A[Setup] --> B[Extract]
B --> C[Save Raw]
C --> D[Transform]
D --> E[Load PostgreSQL]
E --> F[Save Local]
F --> G[Notify]
```

---

# 📊 Data Transformation Rules

The following data quality and business rules are applied:

### Data Cleaning

❌ Remove canceled transactions

❌ Remove null Customer IDs

❌ Remove negative quantities

❌ Remove invalid prices

### Data Standardization

✅ Normalize product descriptions

✅ Standardize customer information

✅ Standardize country names

### Business Logic

✅ Revenue calculation

```python
total_revenue = quantity * unit_price
```

✅ Customer segmentation

✅ Analytical feature generation

---

# 🗄 Target Data Model

## Table: `ecommerce_transacciones`

| Column | Data Type |
|----------|------------|
| invoice_no | VARCHAR |
| stock_code | VARCHAR |
| quantity | INTEGER |
| unit_price | NUMERIC |
| total_venta | NUMERIC |
| customer_id | VARCHAR |
| country | VARCHAR |

---

# 📈 Business Insights

The processed dataset enables:

- Identification of top-selling products
- Revenue analysis by country
- Customer purchasing behavior analysis
- Sales trend monitoring
- Business performance reporting
- Data-driven decision making

---

# 📋 Example Analytical Questions

The pipeline supports answering questions such as:

### Sales Performance

- What are the top 10 best-selling products?
- Which countries generate the highest revenue?
- How do monthly sales evolve over time?

### Customer Analysis

- Which customer segments generate the most revenue?
- What is the average order value by country?

### Returns Analysis

- Which categories generate the highest number of returns?
- What percentage of transactions are cancellations?

---

# 🚀 Future Improvements

- Data Warehouse modeling (Star Schema)
- Dimensional modeling
- Data Quality Dashboard
- Power BI Integration
- Incremental Loads
- CI/CD Deployment
- Cloud Deployment (AWS / Azure)

---

# 👨‍💻 Author

**Juan Camilo Guengue Pérez**

Data Engineering Technical Assessment

RIWI - Cohort 7

---