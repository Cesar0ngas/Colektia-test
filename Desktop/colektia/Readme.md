# Documentación Detallada del Proceso ETL

Este repositorio contiene la implementación completa de un flujo ETL (Extract, Transform, Load) usando Python y MySQL. A continuación se describen en detalle cada uno de los entregables y los pasos realizados.

---

## Entregables

1. **Código Python**: Archivo `test.py` con todo el pipeline.  
2. **Documento de explicación**: Este README en formato Markdown.  
3. **Archivos de salida**:
   - `final_data.csv`
   - `final_data.parquet`

---

## Estructura del codigo:
    1. Extracción
    2. Validación
    3. Unión (JOIN)
    4. Filtrado
    5. Transformación
    6. Agregación
    7. Ordenación
    8. Limpieza
    9. Exportación


## Preparación del Entorno

- **Entorno Conda**: Creado y configuradon con las siguientes herramientas/librerias

conda install pandas sqlalchemy mysql-connector-python pyarrow

- **SQL**: Se uso el workbench para crear la base de datos simulada


# 1. Extraccion
Output:
customers
   id           name country          created_at 
0   1       John Doe     USA 2022-01-05 10:00:00 
1   2     Jane Smith  Canada 2022-02-10 09:30:00 
2   3  David Johnson     USA 2023-05-12 14:45:00 
3   4     Lucy Brown  Mexico 2023-06-01 08:20:00 

orders
   id  customer_id  amount          order_date   
0   1            1  150.50 2022-01-05 11:00:00   
1   2            2  220.00 2022-02-10 11:30:00   
2   3            1   99.99 2023-01-01 10:15:00   
3   4            3  120.10 2023-05-15 12:20:00   
4   5            4   80.00 2023-06-10 09:00:00   

# 2. Exploracion y Validacion
Output:
Customers dimensions:  (4, 4)
Orders dimensions:  (5, 4) 

<class 'pandas.core.frame.DataFrame'>
RangeIndex: 4 entries, 0 to 3
Data columns (total 4 columns):
 #   Column      Non-Null Count  Dtype           
---  ------      --------------  -----           
 0   id          4 non-null      int64           
 1   name        4 non-null      object          
 2   country     4 non-null      object          
 3   created_at  4 non-null      datetime64[ns]  
dtypes: datetime64[ns](1), int64(1), object(2)   
memory usage: 260.0+ bytes
Customers Info:  None
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 5 entries, 0 to 4
Data columns (total 4 columns):
 #   Column       Non-Null Count  Dtype          
---  ------       --------------  -----          
 0   id           5 non-null      int64          
 1   customer_id  5 non-null      int64          
 2   amount       5 non-null      float64        
 3   order_date   5 non-null      datetime64[ns] 
dtypes: datetime64[ns](1), float64(1), int64(2)  
memory usage: 292.0 bytes
Orders Info:  None 

Nulos para Customers:  id            0
name          0
country       0
created_at    0
dtype: int64
Nulos para Orders:  id             0
customer_id    0
amount         0
order_date     0
dtype: int64

# 3. Unir ambas tablas
Output:
Joined DataFrame:
    order_id  customer_id           name country  amount          order_date          created_at
0         1            1       John Doe     USA  150.50 2022-01-05 11:00:00 2022-01-05 10:00:00
1         2            2     Jane Smith  Canada  220.00 2022-02-10 11:30:00 2022-02-10 09:30:00
2         3            1       John Doe     USA   99.99 2023-01-01 10:15:00 2022-01-05 10:00:00
3         4            3  David Johnson     USA  120.10 2023-05-15 12:20:00 2023-05-12 14:45:00
4         5            4     Lucy Brown  Mexico   80.00 2023-06-10 09:00:00 2023-06-01 08:20:00

# 4. Filtrar datos
Output:
USA DataFrame:
    order_id  customer_id           name country  amount          order_date          created_at
0         1            1       John Doe     USA  150.50 2022-01-05 11:00:00 2022-01-05 10:00:00
2         3            1       John Doe     USA   99.99 2023-01-01 10:15:00 2022-01-05 10:00:00
3         4            3  David Johnson     USA  120.10 2023-05-15 12:20:00 2023-05-12 14:45:00
 USA DataFrame dimensions:  (3, 7)

# 5. Transformar datos
Output:
Datos transformados
   order_id  customer_id           name country  amount          order_date year_month  high_value
0         1            1       John Doe     USA  150.50 2022-01-05 11:00:00    2022-01        True
1         2            2     Jane Smith  Canada  220.00 2022-02-10 11:30:00    2022-02        True
2         3            1       John Doe     USA   99.99 2023-01-01 10:15:00    2023-01       False
3         4            3  David Johnson     USA  120.10 2023-05-15 12:20:00    2023-05        True
4         5            4     Lucy Brown  Mexico   80.00 2023-06-10 09:00:00    2023-06       False
Datos transformados dimensions:  (5, 9)

# 6. Agregacion y estadisticas
Output:
AGG by Conuntry:
   country  total_orders  sum_amount  avg_amount
0  Canada             1      220.00      220.00
1  Mexico             1       80.00       80.00
2     USA             3      370.59      123.53

# 7. Ordenar y Clasificar
Output:
Sorted by sum_amount:
   country  total_orders  sum_amount  avg_amount
0     USA             3      370.59      123.53
1  Canada             1      220.00      220.00
2  Mexico             1       80.00       80.00

# 8. Normalizacion
Output:
Negative amounts:
 Empty DataFrame
Columns: [order_id, customer_id, name, country, amount, order_date, created_at, year_month, high_value]
Index: []
Unicos countries:
 ['USA' 'Canada' 'Mexico']
Normalized country:
 ['USA' 'CANADA' 'MEXICO']

