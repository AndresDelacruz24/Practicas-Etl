import os 
import psycopg2 
from dotenv import load_dotenv 
import pandas as pd 
from sqlalchemy import create_engine 
load_dotenv() 

#varaibles globales 
df_1 = None 
engine = create_engine("postgresql+psycopg2://"+os.getenv('DB_POSTGRES_USER')+":"+os.getenv('DB_POSTGRES_PASSWORD')+"@"+os.getenv('DB_POSTGRES_HOST')+":"+os.getenv('DB_POSTGRES_PORT')+"/"+os.getenv('DB_POSTGRES_DATABASE')) 

#connecion a la base de datos 
def conectar_postgres(host, database, user, password, port=5432): 
    try: 
        print("Intentando conectar a PostgreSQL...") 
        conexion = psycopg2.connect( host=host, database=database, user=user, password=password, port=port ) 
        print("Conexión exitosa a PostgreSQL") 
        return conexion 
    except Exception as e: print(f"Error al conectar a PostgreSQL: {e}") 
    return None 

# Crear la conexión a la base de datos 
conn = conectar_postgres( os.getenv('DB_POSTGRES_HOST'), os.getenv('DB_POSTGRES_DATABASE'), os.getenv('DB_POSTGRES_USER'), os.getenv('DB_POSTGRES_PASSWORD'), int(os.getenv('DB_POSTGRES_PORT', 5432)) ) 

ventas = pd.read_sql( """SELECT nombre_cliente, nombre_vendedor, region_vendedor, fecha_factura, cantidad, precio_unitario FROM prueba_sg.ventas""" , engine, parse_dates=['fecha_factura']) 

#traer informacion de la base de datos 
def traer_datos(query): 
    global df_1 
    try: 
        cursor = conn.cursor() 
        cursor.execute(query) 
        resultados = cursor.fetchall() 
        df_1 = pd.DataFrame(resultados) 
        cursor.close() 
        return resultados 
    except Exception as e: 
        print(f"Error al traer datos: {e}") 
        return None 
    
#cargar datos a la base de datos con las dimenciones 
def cargar_datos(query, data): 
    try: 
        cursor = conn.cursor() 
        cursor.executemany(query, data) 
        conn.commit() 
        print("Datos cargados exitosamente") 
        cursor.close() 
    except Exception as e: 
        print(f"Error al cargar datos: {e}") 
        
def cargar_dim_region(): 
    query_consulta = "select distinct(region_vendedor) from prueba_sg.ventas v where region_vendedor is not null" 
    query_insert = "INSERT INTO dm_ventas_prueba.dim_region (region_id, nombre_region) VALUES (%s, %s)" 
    
    data = traer_datos(query_consulta) 
    data = [(i+1, region[0]) for i, region in enumerate(data)] 
    cargar_datos(query_insert, data) 
    
    
def cargar_dim_ciudad(): 
    query_consulta = "select distinct(ciudad_id) from prueba_sg.ventas v where ciudad_id is not null" 
    query_insert = "INSERT INTO dm_ventas_prueba.dim_ciudad (ciudad_id) VALUES (%s)" 
    data = traer_datos(query_consulta) 
    cargar_datos(query_insert, data) 
    
def cargar_dim_cliente(): 
    query_consulta1 = "select distinct(nombre_cliente) from prueba_sg.ventas v where nombre_cliente is not null" 
    query_consulta2 = "select distinct(nombre_cliente) from prueba_sg.presupuesto p where nombre_cliente is not null" 
    query_insert = "INSERT INTO dm_ventas_prueba.dim_cliente (cliente_id, nombre_cliente) VALUES (%s, %s)" 
    query_existe = "SELECT nombre_cliente FROM dm_ventas_prueba.dim_cliente WHERE nombre_cliente = %s" 
    data = traer_datos(query_consulta1) + traer_datos(query_consulta2) 
    cursor = conn.cursor() 
    data_filtrada = [] 
    for nombre_cliente in data: 
        cursor.execute(query_existe, (nombre_cliente,)) 
        if cursor.fetchone() is None: 
            data_filtrada.append((nombre_cliente,)) 
    cursor.close() 
    if data_filtrada: 
        data_filtrada = [(i+1, data[0]) for i, data in enumerate(data_filtrada)] 
        cargar_datos(query_insert, data_filtrada) 
    else: print("No hay clientes nuevos para insertar") 
    
def cargar_vendedor():
    # Consultas origen
    query_consulta_ventas = """
        SELECT DISTINCT nombre_vendedor, region_vendedor
        FROM prueba_sg.ventas
        WHERE nombre_vendedor IS NOT NULL
          AND region_vendedor IS NOT NULL
    """

    query_consulta_presupuesto = """
        SELECT DISTINCT nombre_vendedor
        FROM prueba_sg.presupuesto
        WHERE nombre_vendedor IS NOT NULL
    """

    # Consultas de validación
    query_existe_vendedor = """
        SELECT vendedor_id
        FROM dm_ventas_prueba.dim_vendedor
        WHERE nombre_vendedor = %s
    """

    query_existe_region = """
        SELECT region_id
        FROM dm_ventas_prueba.dim_region
        WHERE nombre_region = %s
    """

    # Insert
    query_insert = """
        INSERT INTO dm_ventas_prueba.dim_vendedor (
            vendedor_id,
            nombre_vendedor,
            region
        ) VALUES (%s, %s, %s)
    """

    # Obtener datos origen
    data = traer_datos(query_consulta_ventas) + traer_datos(query_consulta_presupuesto)

    cursor_vendedor = conn.cursor()
    cursor_region = conn.cursor()

    data_filtrada = []

    for vendedor_data in data:
        nombre_vendedor = vendedor_data[0]
        nombre_region = vendedor_data[1] if len(vendedor_data) > 1 else None

        print(f"Procesando vendedor: {nombre_vendedor}, región: {nombre_region}")

        # Verificar si el vendedor ya existe
        cursor_vendedor.execute(query_existe_vendedor, (nombre_vendedor,))
        existe_vendedor = cursor_vendedor.fetchone()

        # Buscar región si viene informada
        region_id = None
        if nombre_region is not None:
            cursor_region.execute(query_existe_region, (nombre_region,))
            region_result = cursor_region.fetchone()
            region_id = region_result[0] if region_result else None

        # Insertar solo si no existe
        if existe_vendedor is None:
            data_filtrada.append((nombre_vendedor, region_id))

    cursor_vendedor.close()
    cursor_region.close()

    # Insertar nuevos vendedores
    if data_filtrada:
        data_final = [
            (i + 1, row[0], row[1])
            for i, row in enumerate(data_filtrada)
        ]
        cargar_datos(query_insert, data_final)
    else:
        print("No hay vendedores nuevos para insertar") 

def cargar_dim_tiempo():   
    print("Datos de ventas cargados para procesar la dimensión tiempo:")   
    dim_tiempo = ventas[['fecha_factura']].drop_duplicates()   
    dim_tiempo['anno'] = dim_tiempo['fecha_factura'].dt.year   
    dim_tiempo['mes'] = dim_tiempo['fecha_factura'].dt.month   
    dim_tiempo['dia'] = dim_tiempo['fecha_factura'].dt.day   
    dim_tiempo.rename(columns={'fecha_factura': 'fecha'}, inplace=True)   
    dim_tiempo.to_sql( 'dim_tiempo', engine, schema='dm_ventas_prueba', if_exists='append', index=False )   

def cargar_fact_ventas():

    query_fact = """
        SELECT
            c.cliente_id,
            vendedor.vendedor_id,
            r.region_id,
            ciudad.ciudad_id,
            t.tiempo_id,
            v.cantidad,
            v.precio_unitario
        FROM prueba_sg.ventas v
        JOIN dm_ventas_prueba.dim_cliente c ON v.nombre_cliente = c.nombre_cliente
        JOIN dm_ventas_prueba.dim_region r ON v.region_vendedor = r.nombre_region
        JOIN dm_ventas_prueba.dim_ciudad ciudad ON v.ciudad_id = ciudad.ciudad_id
        JOIN dm_ventas_prueba.dim_vendedor vendedor ON v.nombre_vendedor = vendedor.nombre_vendedor
        JOIN dm_ventas_prueba.dim_tiempo t ON v.fecha_factura = t.fecha
    """

    # Leer los datos ya transformados (IDs + métricas)
    ventas_fact_df = pd.read_sql(query_fact, engine)

    # Insertar en la tabla fact (por lotes)
    ventas_fact_df.to_sql(
        name='fact_ventas',
        con=engine,
        schema='dm_ventas_prueba',
        if_exists='append',
        index=False,
        method='multi',
        chunksize=10000
    )




# ===============================
# EJECUCIÓN DEL ETL
# ===============================

if __name__ == "__main__":

    # 1. Cargar dimensiones
    # cargar_dim_region()
    # cargar_dim_ciudad()
    # cargar_dim_cliente()
    #cargar_vendedor()
    #cargar_dim_tiempo()

    # 2. Cargar fact
    cargar_fact_ventas()