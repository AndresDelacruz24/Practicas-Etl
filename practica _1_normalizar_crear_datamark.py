import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

#varaibles globales
df_1 = None


#connecion a la base de datos
def conectar_postgres(host, database, user, password, port=5432): 
    try: 
        print("Intentando conectar a PostgreSQL...")
        conexion = psycopg2.connect( host=host, database=database, user=user, password=password, port=port ) 
        print("Conexión exitosa a PostgreSQL") 
        return conexion 
    except Exception as e: 
        print(f"Error al conectar a PostgreSQL: {e}") 
        return None 


# Crear la conexión a la base de datos 
conn = conectar_postgres( 
                    os.getenv('DB_POSTGRES_HOST'), 
                    os.getenv('DB_POSTGRES_DATABASE'), 
                    os.getenv('DB_POSTGRES_USER'), 
                    os.getenv('DB_POSTGRES_PASSWORD'), 
                    int(os.getenv('DB_POSTGRES_PORT', 5432)) 
                    )



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
        cursor.close()
        print("Datos cargados exitosamente")
    except Exception as e:
        print(f"Error al cargar datos: {e}")



def cargar_dim_region():
    
    query_consulta = "select distinct(region_vendedor) from  prueba_sg.ventas v where region_vendedor is not null"
    query_insert = "INSERT INTO dm_ventas_prueba.dim_region (region_id, nombre_region) VALUES (%s, %s)"

    data =traer_datos(query_consulta)

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
    else:
        print("No hay clientes nuevos para insertar")


def cargar_vendedor():
    query_consulta1 = "select distinct(nombre_vendedor),region_vendedor from  prueba_sg.ventas v where nombre_vendedor is not null and region_vendedor is not null"
    query_consulta2 = "select distinct(nombre_vendedor) from  prueba_sg.presupuesto p where nombre_vendedor is not null"
    query_insert = "INSERT INTO dm_ventas_prueba.dim_vendedor (vendedor_id, nombre_vendedor, region_id) VALUES (%s, %s, %s)"
    query_existe = "SELECT nombre_vendedor FROM dm_ventas_prueba.dim_vendedor WHERE nombre_vendedor = %s"
    query_existe_region = "SELECT nombre_region FROM dm_ventas_prueba.dim_region WHERE nombre_region = %s"

    data1 = traer_datos(query_consulta1) + traer_datos(query_consulta2)

    print("Datos obtenidos de las consultas:")
    print("Consulta 1:", data1)

    cursor = conn.cursor()
    
    data_filtrada = []
    
    for vendedor_data in data1:
        nombre_vendedor = vendedor_data[0]
        nombre_region = vendedor_data[1] if len(vendedor_data) > 1 else None
        
        cursor.execute(query_existe, (nombre_vendedor,))

        if cursor.fetchone() is None and nombre_region is not None:
            cursor.execute(query_existe_region, (nombre_region,))
            if cursor.fetchone() is not None:
                data_filtrada.append((nombre_vendedor, nombre_region))

    cursor.close()

    if data_filtrada:
        data_filtrada = [(i+1, data[0], data[1]) for i, data in enumerate(data_filtrada)]
        cargar_datos(query_insert, data_filtrada)
    else:
        print("No hay vendedores nuevos para insertar")

#inicio del programa
#ejemplo de consulta
# traer_datos(query = "SELECT * FROM prueba_sg.ventas")
# print(df_1)
#cargar_dim_region()
#cargar_dim_ciudad()
#cargar_dim_cliente()
cargar_vendedor()