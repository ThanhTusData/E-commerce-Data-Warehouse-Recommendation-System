from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from transform_dim_customers import transform_dim_customers
from transform_dim_products import transform_dim_products
from transform_dim_sellers import transform_dim_sellers
from transform_dim_geolocation import transform_dim_geolocation
from transform_dim_dates import transform_dim_dates
from transform_dim_payments import transform_dim_payments
from transform_fact_orders import transform_fact_orders
from extract_data import extract_and_load_to_staging

# FIXED: Cấu hình timeout và retry
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # CHANGED: Tăng retry lên 1
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),  # ADDED: Timeout 60 phút
    'catchup': False,  # ADDED: Không chạy các run bị missed
}

with DAG(
    'e_commerce_dw_etl',
    default_args=default_args,
    description='ETL process for E-commerce Data Warehouse',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,  # ADDED: Chỉ cho phép 1 run cùng lúc
    tags=['etl', 'ecommerce', 'data_warehouse']  # ADDED: Tags để dễ quản lý
) as dag:

    # Task Extract - FIXED: Thêm timeout riêng cho extract task
    with TaskGroup("extract") as extract_group:
        extract_task = PythonOperator(
            task_id='extract_and_load_to_staging',
            python_callable=extract_and_load_to_staging,
            provide_context=True,
            execution_timeout=timedelta(minutes=45),  # ADDED: Timeout riêng 45 phút
            pool='mysql_pool',  # ADDED: Sử dụng connection pool (optional)
        )

    # Task Group Transform
    with TaskGroup("transform") as transform_group:
        task_dim_customers = PythonOperator(
            task_id='transform_dim_customers',
            python_callable=transform_dim_customers,
            execution_timeout=timedelta(minutes=15),
        )
        
        task_dim_products = PythonOperator(
            task_id='transform_dim_products',
            python_callable=transform_dim_products,
            execution_timeout=timedelta(minutes=15),
        )
        
        task_dim_sellers = PythonOperator(
            task_id='transform_dim_sellers',
            python_callable=transform_dim_sellers,
            execution_timeout=timedelta(minutes=10),
        )
        
        task_dim_geolocation = PythonOperator(
            task_id='transform_dim_geolocation',
            python_callable=transform_dim_geolocation,
            execution_timeout=timedelta(minutes=10),
        )
        
        task_dim_dates = PythonOperator(
            task_id='transform_dim_dates',
            python_callable=transform_dim_dates,
            execution_timeout=timedelta(minutes=10),
        )
        
        task_dim_payments = PythonOperator(
            task_id='transform_dim_payments',
            python_callable=transform_dim_payments,
            execution_timeout=timedelta(minutes=10),
        )

    # Task Group Load
    with TaskGroup("load") as load_group:
        task_fact_orders = PythonOperator(
            task_id='transform_fact_orders',
            python_callable=transform_fact_orders,
            execution_timeout=timedelta(minutes=20),
        )

    # Thiết lập dependencies
    extract_group >> transform_group >> load_group

# OPTIONAL: Thêm task kiểm tra kết nối trước khi chạy
# def test_connections(**kwargs):
#     """Test MySQL và PostgreSQL connections"""
#     from mysql_operator import MySQLOperators
#     from postgresql_operator import PostgresOperators
    
#     try:
#         mysql_op = MySQLOperators('mysql')
#         postgres_op = PostgresOperators('postgres')
        
#         # Test query đơn giản
#         mysql_result = mysql_op.get_data_to_pd("SELECT 1")
#         print(f"MySQL connection OK: {len(mysql_result)} row")
        
#         postgres_op.save_data_to_postgres(
#             mysql_result, 
#             'test_connection',
#             schema='staging',
#             if_exists='replace'
#         )
#         print("PostgreSQL connection OK")
        
#     except Exception as e:
#         print(f"Connection test failed: {str(e)}")
#         raise

# Uncomment nếu muốn thêm task test connection
# test_conn_task = PythonOperator(
#     task_id='test_connections',
#     python_callable=test_connections,
#     dag=dag
# )
# 
# test_conn_task >> extract_group