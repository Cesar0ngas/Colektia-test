import pandas as pd
from sqlalchemy import create_engine

def main():
    # Conectarse a SQL
    engine = create_engine("mysql+mysqlconnector://root:190503@127.0.0.1:3306/test_db")
    
    # Leer los datos de SQL
    df_customers = pd.read_sql("SELECT * FROM customers", engine)
    df_orders = pd.read_sql("SELECT * FROM orders", engine)
    
    # Tarea 1
    print("customers")
    # Mostrar los datos para customers
    print(df_customers, "\n")
    
    print("orders")
    # Mostrar los datos para orders 
    print(df_orders, "\n")
    
    
    # Tarea 2
    # 2.1 Dimesiones
    print("Customers dimensions: ", df_customers.shape)
    print("Orders dimensions: ", df_orders.shape, "\n")
    
    # 2.2 Info
    print("Customers Info: ", df_customers.info())
    print("Orders Info: ", df_orders.info(), "\n")
    
    # 2.3 Nulos
    print("Nulos para Customers: ", df_customers.isnull().sum())
    print("Nulos para Orders: ", df_orders.isnull().sum())
    
    
    # Tarea 3 
    df_joined = pd.merge(
        df_orders,
        df_customers,
        left_on = "customer_id",
        right_on = "id",
        how = "inner",
        suffixes = ("_order", "_customer")
    )
    
    df_joined = df_joined[
        [
            "id_order", "customer_id", "name", "country", "amount", "order_date", "created_at"
        ]
    ].rename(columns = {"id_order": "order_id"})
    
    print("Joined DataFrame: ", "\n", df_joined, "\n")
    print("Joined DataFrame dimensions: ", df_joined.shape)
    
    
    # Tarea 4
    df_usa = df_joined[df_joined["country"] == "USA"].copy()
    
    print("USA DataFrame: ", "\n", df_usa, "\n", "USA DataFrame dimensions: ",df_usa.shape)
    
    
    # Tarea 5
    # 5.1 columna year_month en formato
    df_joined["year_month"] = df_joined["order_date"].dt.to_period("M").astype(str)
    
    # 5.2 columna booleana si > 100USD
    df_joined["high_value"] = df_joined["amount"] > 100.0
    
    print("Datos transformados")
    print(df_joined[[
        "order_id", "customer_id", "name", "country", "amount", "order_date", "year_month", "high_value"
    ]])
    print("Datos transformados dimensions: ", df_joined.shape)
    
    
    # Tarea 6
    stats = (
        df_joined.groupby("country").agg(
            total_orders = ("order_id", "count"),
            sum_amount = ("amount", "sum"),
            avg_amount = ("amount", "mean"),
        )
        .reset_index()        
    )
    print("AGG by Conuntry: ", "\n", stats)
    
    
    # Tarea 7
    stats_sorted = stats.sort_values("sum_amount", ascending = False).reset_index(drop = True)
    
    print("Sorted by sum_amount: ", "\n", stats_sorted)
    
    
    # Tarea 8
    # 8.1.0 Inconsistencias
    # 8.1.1 Negativos
    neg_amounts = df_joined[df_joined["amount"] < 0]
    print("Negative amounts: ", "\n", neg_amounts)
    
    # 8.1.2 espacios o mayusculas inconsitentes
    unicos_countries = df_joined["country"].unique()
    print("Unicos countries: ", "\n", unicos_countries)
    
    # 8.2.0 Normalizacion
    df_joined["country"] = df_joined["country"].str.strip().str.upper()
    
    print("Normalized country: ", "\n", df_joined["country"].unique())
    
    
    # Tarea 9
    # Exportar a CSV
    csv_path = "final data.csv"
    df_joined.to_csv(csv_path, index = False)
    print("CSV saved to: ", csv_path)
    
    # Exportar a Parquet
    parquet_path = "final data.parquet"
    df_joined.to_parquet(parquet_path, index = False)
    print("Parquet saved to: ", parquet_path)
    
    
if __name__ == "__main__":
    main()