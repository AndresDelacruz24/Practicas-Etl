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
    except Exception as e:
        print(f"Error al traer datos: {e}")
        return None
    

#cargar datos a la base de datos con las dimenciones

#inicio del programa
#ejemplo de consulta
traer_datos(query = "SELECT * FROM etl.ventas")
print(df_1)