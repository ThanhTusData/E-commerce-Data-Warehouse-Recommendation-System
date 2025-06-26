from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import logging

from mysql_operator import MySQLOperators
from postgresql_operator import PostgresOperators


def extract_and_load_to_staging(**kwargs):
    source_operator = MySQLOperators('mysql')
    staging_operator = PostgresOperators('postgres')
    
    tables = [
        "prod_cate_name_trans",
        "geolocation", 
        "sellers",
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "order_reviews"
    ]
    
    for table in tables:
        try:
            logging.info(f"Bắt đầu trích xuất bảng {table}")
            
            # SOLUTION 1: Phân trang để tránh timeout
            batch_size = 5000  # Giảm xuống 5k để an toàn hơn
            offset = 0
            total_records = 0
            
            # Xóa bảng cũ trước khi load (chỉ lần đầu)
            first_batch = True
            
            while True:
                # Query với LIMIT và OFFSET
                query = f"SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}"
                
                logging.info(f"Đang xử lý batch {offset//batch_size + 1} của bảng {table}")
                
                # Trích xuất dữ liệu từ nguồn MySQL
                df = source_operator.get_data_to_pd(query)
                
                # Nếu không còn data thì break
                if df.empty:
                    logging.info(f"Không còn data cho bảng {table}")
                    break
                
                # Lưu dữ liệu vào PostgreSQL staging
                if first_batch:
                    # Lần đầu: replace để xóa data cũ
                    staging_operator.save_data_to_postgres(
                        df,
                        f"stg_{table}",
                        schema='staging',
                        if_exists='replace'
                    )
                    first_batch = False
                else:
                    # Các lần sau: append để thêm data
                    staging_operator.save_data_to_postgres(
                        df,
                        f"stg_{table}", 
                        schema='staging',
                        if_exists='append'
                    )
                
                total_records += len(df)
                offset += batch_size
                
                logging.info(f"Đã xử lý {total_records} records cho bảng {table}")
                
                # Nếu batch nhỏ hơn batch_size thì đã hết data
                if len(df) < batch_size:
                    logging.info(f"Batch cuối cho bảng {table}")
                    break
            
            logging.info(f"Hoàn thành trích xuất bảng {table}: {total_records} records")
            print(f"Đã trích xuất và lưu bảng {table} từ MySQL vào PostgreSQL staging - Total: {total_records} records")
            
        except Exception as e:
            logging.error(f"Lỗi khi xử lý bảng {table}: {str(e)}")
            print(f"Lỗi khi xử lý bảng {table}: {str(e)}")
            # Continue với bảng tiếp theo thay vì raise exception
            continue


# ALTERNATIVE: Nếu muốn chỉ load incremental data  
def extract_and_load_incremental(**kwargs):
    source_operator = MySQLOperators('mysql')
    staging_operator = PostgresOperators('postgres')
    
    # Các bảng có timestamp để load incremental
    timestamped_tables = {
        "orders": "order_purchase_timestamp",
        "order_items": "shipping_limit_date", 
        "payments": "created_at",  # Giả sử có cột này
        "order_reviews": "review_creation_date"
    }
    
    # Bảng nhỏ load full
    small_tables = ["prod_cate_name_trans", "geolocation", "sellers", "customers", "products"]
    
    from datetime import datetime, timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Load incremental cho bảng lớn
    for table, timestamp_col in timestamped_tables.items():
        try:
            query = f"""
            SELECT * FROM {table} 
            WHERE DATE({timestamp_col}) >= '{yesterday}'
            """
            
            df = source_operator.get_data_to_pd(query)
            
            if not df.empty:
                staging_operator.save_data_to_postgres(
                    df,
                    f"stg_{table}",
                    schema='staging', 
                    if_exists='replace'  # Hoặc 'append' tùy logic
                )
                print(f"Đã load incremental {len(df)} records cho bảng {table}")
        except Exception as e:
            print(f"Lỗi incremental load bảng {table}: {str(e)}")
    
    # Load full cho bảng nhỏ
    for table in small_tables:
        try:
            df = source_operator.get_data_to_pd(f"SELECT * FROM {table}")
            staging_operator.save_data_to_postgres(
                df,
                f"stg_{table}",
                schema='staging',
                if_exists='replace'
            )
            print(f"Đã load full {len(df)} records cho bảng {table}")
        except Exception as e:
            print(f"Lỗi full load bảng {table}: {str(e)}")