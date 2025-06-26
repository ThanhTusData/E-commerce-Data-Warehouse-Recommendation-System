import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate

class PostgreSQLDataReader:
    def __init__(self, host='localhost', port=5432, database='your_db', 
                 username='your_user', password='your_password'):
        """
        Khởi tạo kết nối PostgreSQL
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        
        # Tạo connection string
        self.connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.connection_string)
        
    def test_connection(self):
        """Test kết nối PostgreSQL"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT version()")
                print("✅ Kết nối PostgreSQL thành công!")
                print(f"Database version: {result.fetchone()[0]}")
                return True
        except Exception as e:
            print(f"❌ Lỗi kết nối: {e}")
            return False
    
    def get_all_tables(self, schema='warehouse'):
        """Lấy danh sách tất cả bảng trong schema"""
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}'
        ORDER BY table_name;
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            tables = df['table_name'].tolist()
            print(f"📋 Có {len(tables)} bảng trong schema '{schema}':")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
            return tables
        except Exception as e:
            print(f"❌ Lỗi lấy danh sách bảng: {e}")
            return []
    
    def get_table_info(self, table_name, schema='warehouse'):
        """Lấy thông tin cấu trúc bảng"""
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            character_maximum_length
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            print(f"\n📊 Cấu trúc bảng '{table_name}':")
            print(tabulate(df, headers=df.columns, tablefmt='grid', showindex=False))
            return df
        except Exception as e:
            print(f"❌ Lỗi lấy thông tin bảng: {e}")
            return pd.DataFrame()
    
    def get_table_data(self, table_name, schema='warehouse'):
        """Lấy dữ liệu từ bảng"""
        query = f"SELECT * FROM {schema}.{table_name}"
        
        try:
            df = pd.read_sql(query, self.engine)
            print(f"\n📈 Dữ liệu bảng '{table_name}':")
            print(f"Tổng số cột: {len(df.columns)}")
            print(f"Số dòng lấy được: {len(df)}")
            print("\n" + "="*80)
            # print(tabulate(df, headers=df.columns, tablefmt='grid', showindex=True))
            return df
        except Exception as e:
            print(f"❌ Lỗi lấy dữ liệu: {e}")
            return pd.DataFrame()
    
    def get_table_count(self, table_name, schema='warehouse'):
        """Đếm số dòng trong bảng"""
        query = f"SELECT COUNT(*) as total_rows FROM {schema}.{table_name}"
        
        try:
            df = pd.read_sql(query, self.engine)
            count = df['total_rows'].iloc[0]
            print(f"📊 Bảng '{table_name}' có {count:,} dòng dữ liệu")
            return count
        except Exception as e:
            print(f"❌ Lỗi đếm dữ liệu: {e}")
            return 0
    
    def get_table_summary(self, table_name, schema='warehouse'):
        """Lấy thống kê tổng quan bảng"""
        df = self.get_table_data(table_name, schema)
        
        if not df.empty:
            print(f"\n📊 Thống kê bảng '{table_name}':")
            print(f"Shape: {df.shape}")
            print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
            
            # Thống kê cho cột số
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                print(f"\n🔢 Thống kê cột số:")
                print(df[numeric_cols].describe())
            
            # Thông tin missing values
            missing = df.isnull().sum()
            if missing.sum() > 0:
                print(f"\n⚠️  Missing values:")
                for col, miss_count in missing[missing > 0].items():
                    print(f"  {col}: {miss_count} ({miss_count/len(df)*100:.1f}%)")
        
        return df
    
    def export_to_csv(self, table_name, schema='warehouse', output_file=None):
        """Export bảng ra file CSV"""
        if output_file is None:
            output_file = f"{table_name}_export.csv"
        
        query = f"SELECT * FROM {schema}.{table_name}"
        
        try:
            df = pd.read_sql(query, self.engine)
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"✅ Đã export {len(df)} dòng từ bảng '{table_name}' ra file '{output_file}'")
            return output_file
        except Exception as e:
            print(f"❌ Lỗi export: {e}")
            return None
    
    def run_custom_query(self, query):
        """Chạy query tùy chỉnh"""
        try:
            df = pd.read_sql(query, self.engine)
            print(f"✅ Query thực hiện thành công. Kết quả: {df.shape}")
            print(tabulate(df, headers=df.columns, tablefmt='grid', showindex=True))
            return df
        except Exception as e:
            print(f"❌ Lỗi thực hiện query: {e}")
            return pd.DataFrame()


# ====================== MAIN USAGE ======================
def main():
    """Hàm chính để sử dụng"""
    
    # 1. Cấu hình kết nối (THAY ĐỔI THEO SETUP CỦA BẠN)
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'your_database_name',  # Thay đổi
        'username': 'your_username',       # Thay đổi
        'password': 'your_password'        # Thay đổi
    }
    
    # 2. Khởi tạo reader
    reader = PostgreSQLDataReader(**db_config)
    
    # 3. Test kết nối
    if not reader.test_connection():
        print("Không thể kết nối database. Kiểm tra lại config!")
        return
    
    # 4. Lấy danh sách tất cả bảng
    tables = reader.get_all_tables()
    
    if not tables:
        print("Không tìm thấy bảng nào trong warehouse schema!")
        return
    
    # 5. Chọn bảng để xem (có thể thay đổi)
    print("\n" + "="*80)
    table_to_view = input(f"Nhập tên bảng muốn xem (hoặc Enter để xem '{tables[0]}'): ").strip()
    
    if not table_to_view:
        table_to_view = tables[0]
    
    if table_to_view not in tables:
        print(f"❌ Bảng '{table_to_view}' không tồn tại!")
        return
    
    # 6. Hiển thị chi tiết bảng
    print(f"\n🔍 PHÂN TÍCH BẢNG: {table_to_view}")
    print("="*80)
    
    # Thông tin cấu trúc
    reader.get_table_info(table_to_view)
    
    # Đếm số dòng
    reader.get_table_count(table_to_view)
    
    # Lấy dữ liệu mẫu
    df = reader.get_table_data(table_to_view)
    
    # Thống kê tổng quan
    reader.get_table_summary(table_to_view)
    
    # 7. Tùy chọn export
    export_choice = input("\nBạn có muốn export bảng này ra CSV? (y/n): ").strip().lower()
    if export_choice == 'y':
        reader.export_to_csv(table_to_view)
    
    # 8. Tùy chọn chạy query custom
    custom_query = input("\nNhập custom query (hoặc Enter để bỏ qua): ").strip()
    if custom_query:
        reader.run_custom_query(custom_query)
    
    print("\n✅ Hoàn thành!")


# ====================== QUICK FUNCTIONS ======================
def quick_view_table(table_name):
    """Hàm nhanh để xem 1 bảng"""
    db_config = {
        'host': 'localhost',
        'port': 5432, 
        'database': 'your_database_name',  # THAY ĐỔI
        'username': 'your_username',       # THAY ĐỔI  
        'password': 'your_password'        # THAY ĐỔI
    }
    
    reader = PostgreSQLDataReader(**db_config)
    return reader.get_table_data(table_name)


def quick_query(query):
    """Hàm nhanh để chạy query"""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'your_database_name',  # THAY ĐỔI
        'username': 'your_username',       # THAY ĐỔI
        'password': 'your_password'        # THAY ĐỔI  
    }
    
    reader = PostgreSQLDataReader(**db_config)
    return reader.run_custom_query(query)


if __name__ == "__main__":
    main()


# ====================== EXAMPLES ======================
"""
CÁCH SỬ DỤNG:

1. Cài đặt thư viện cần thiết:
   pip install pandas psycopg2-binary sqlalchemy tabulate matplotlib seaborn

2. Thay đổi thông tin kết nối database trong db_config

3. Chạy script:
   python postgres_reader.py

4. Hoặc sử dụng trong Jupyter:
   
   from postgres_reader import PostgreSQLDataReader
   
   reader = PostgreSQLDataReader(
       host='localhost',
       database='your_db', 
       username='user',
       password='pass'
   )
   
   # Xem tất cả bảng
   tables = reader.get_all_tables()
   
   # Xem dữ liệu bảng cụ thể
   df = reader.get_table_data('stg_orders')
   
   # Chạy query tùy chỉnh
   result = reader.run_custom_query('''
       SELECT COUNT(*) as total_orders, 
              AVG(payment_value) as avg_payment
       FROM warehouse.stg_orders o
       JOIN warehouse.stg_payments p ON o.order_id = p.order_id
   ''')

5. Quick usage:
   df = quick_view_table('stg_products')
   result = quick_query('SELECT * FROM warehouse.fact_orders ')
"""