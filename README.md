# E-commerce Data Warehouse Recommendation System

## 🔍 Overview

This project is an end-to-end **E-commerce Recommendation System** built on a **Data Warehouse architecture**. It integrates MySQL and PostgreSQL using Apache Airflow for orchestration, and Power BI for dashboarding. The recommender engine is based on collaborative filtering (KNN, matrix factorization).

---

## 📁 Project Structure

- `config/` – Airflow and database configurations
- `dags/` – Airflow DAGs for ETL pipelines
- `dataset/` – Raw data files
- `load_dataset_into_mysql/` – SQL files for schema creation and data loading into MySQL
- `plugins/` – Custom Airflow operators/hooks
- `logs/` – Airflow log files
- `photo/` – Images for reports/dashboards
- `.env` – Environment variable settings
- `docker-compose.yaml` – Docker multi-service setup (Airflow, MySQL, PostgreSQL)
- `Dockerfile` – Environment setup for custom services
- `mf_knn_recommender.py` – Model building using collaborative filtering
- `use_model.py` – Script to use and evaluate the trained recommender
- `postgres_reader.py` – PostgreSQL query scripts
- `Dashboard.pbix` – Power BI dashboard
- `requirements.txt` – Python dependencies

---

## 🚀 Getting Started

### 1. Start Docker

```bash
docker-compose up -d 
docker ps
```

---

### 2. Initialize MySQL Database

#### Create Schema

```bash
docker exec -it mysql mysql --local-infile=1 -uroot -padmin olist -e "source /load_dataset_into_mysql/olist.sql"
```

#### Load Data

```bash
docker exec -it mysql mysql --local-infile=1 -uroot -padmin olist -e "source /load_dataset_into_mysql/load_data.sql"
```

#### Verify Load

```bash
docker exec -it mysql mysql -uroot -padmin -e "USE olist; SELECT COUNT(*) FROM customers;"
```

---

### 3. Setup Airflow Connections

#### Remove old connections

```bash
docker exec -it air-airflow-webserver-1 airflow connections delete 'mysql'
docker exec -it air-airflow-webserver-1 airflow connections delete 'postgres'
```

#### Add new connections

```bash
docker exec -it air-airflow-webserver-1 airflow connections add 'mysql' --conn-uri 'mysql://admin:admin@mysql:3306/olist'
docker exec -it air-airflow-webserver-1 airflow connections add 'postgres' --conn-uri 'postgresql://admin:admin@de_psql:5432/postgres?sslmode=disable'
```

---

### 4. Initialize PostgreSQL Schemas

```bash
docker exec -i de_psql psql -U admin -d postgres -c "CREATE SCHEMA IF NOT EXISTS staging; CREATE SCHEMA IF NOT EXISTS warehouse;"
```

#### Verify schemas and tables

```bash
docker exec -i de_psql psql -U admin -d postgres -c "\dn"
docker exec -i de_psql psql -U admin -d postgres -c "\dt staging.*"
```

---

### 5. Create Airflow Pool (UI)

- Go to **Airflow Web UI**
- Navigate: `Admin → Pools → +`
  - **Pool Name:** `mysql_pool`
  - **Slots:** `3`
  - **Description:** `MySQL connection pool`
- Click **Save**

---

### 6. Connect Power BI

- **Server:** `localhost:5433`
- **Database:** `postgres`
- **Username:** `admin`
- **Password:** `admin`

---

## 📊 Dashboarding

The Power BI report (`Dashboard.pbix`) connects directly to the PostgreSQL `warehouse` schema and displays insights such as:
- Top selling products
- Revenue by region and time
- Customer segmentation
- Product recommendation effectiveness

---

## 🤖 Machine Learning

Recommender system includes:
- `mf_knn_recommender.py`: Matrix Factorization + KNN
- `use_model.py`: Apply and evaluate recommendation logic
- `postgres_reader.py`: Pull data from PostgreSQL for modeling

---

## 📦 Requirements

Install dependencies (if running locally):

```bash
pip install -r requirements.txt
```

---

## 🧠 Author's Note

This system is highly modular and Dockerized for reproducibility, scalability, and cloud deployment readiness. Extend the pipeline with more DAGs or plug new BI tools as needed.

