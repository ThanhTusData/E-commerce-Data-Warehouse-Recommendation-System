FROM apache/airflow:2.9.2-python3.11


# Switch to root user to install system packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-mysql-client \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-mysql==5.5.3 \
    apache-airflow-providers-postgres==5.9.0 \
    pymysql==1.1.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4 \
    numpy==1.24.4 \
    sqlalchemy==1.4.53