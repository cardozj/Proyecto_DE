#ENTREGABLE 2 CON LAS CORRECCIONES 
import json
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import psycopg2.extensions
from psycopg2.extras import execute_values

def get_data_api():
    """Realiza la solicitud a la API"""
    response = requests.get('https://api.coincap.io/v2/assets')
    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
    # Devuelve los datos en formato JSON
        return response.json()
    else:
    # Si la solicitud falla, imprime un mensaje de error
        print("Error al obtener datos de la API:", response.status_code)
        return None
    
def execute_read_query(connection, query):
    #Conexion a db mediante cursor
    c = connection.cursor()
    result = None
    try:
        #Ejecuto Query para visualizar tabla
        c.execute(query)
        #Guardo resultado y devuelvo a main
        result = c.fetchall()
        return result
    except Exception as e:
        #En el caso de fallar, me devuelve error especifico
        print({e})

# Obtener los datos desde la API y Normalizarlos
df_coincap = get_data_api()
df_coincap = pd.json_normalize(df_coincap, record_path =['data'], meta=['timestamp'])

# Limpieza de datos
df_coincap = df_coincap.drop(columns=['rank'])
df_coincap['timestamp'] = pd.to_datetime(df_coincap['timestamp'])

#Ordeno por precio en el mercado
df_coincap.sort_values(by='priceUsd',ascending=False)

print (df_coincap)

#Carga de credenciales desde .env
load_dotenv()
user = os.getenv("USER")
pwd = os.getenv("PWD")
host = os.getenv("HOST")
db = os.getenv("DB")

#Conexion a la base de datos
try:
    conn = psycopg2.connect(
        host=host,
        dbname=db,
        user=user,
        password=pwd,
        port='5439'
    )
    #En el caso de ser exitosa la conexion
    print("Conectado a Redshift")
except Exception as error:
    #Si fallara la conexion nos devolveria el problema
    print("Error de conexion a Redshift")
    print(error)

#Envio de df
#Conversion de df a tupla
tuplas = [tuple(x) for x in df_coincap.to_numpy()]

#Conexion a db 
cur = conn.cursor()

#Query para insertar valores
insert_query = """INSERT INTO criptos_price (
                                                id, 
                                                symbol, 
                                                name, 
                                                supply, 
                                                maxSupply, 
                                                marketCapUsd, 
                                                volumeUsd24Hr, 
                                                priceUsd, 
                                                changePercent24Hr, 
                                                vwap24Hr, 
                                                explorer, 
                                                timestamp) 
                                            VALUES %s"""

#Crear la tabla si no existe
cur.execute("""
            CREATE TABLE IF NOT EXISTS criptos_price(
	        id varchar(40) NOT NULL,
	        symbol varchar (20) UNIQUE,
	        name varchar (100),
	        supply decimal (20,2),
	        maxSupply decimal (20,2),
 	        marketCapUsd decimal (20,2),
	        volumeUsd24Hr decimal (20,2),
	        priceUsd decimal (20,2),
	        changePercent24Hr decimal (20,2),
	        vwap24Hr decimal (20,2),
	        explorer varchar (120),
	        timestamp timestamp,
	        PRIMARY key (id)
            )
        """)

#Limpio tabla para actualizar contenido
cur.execute("TRUNCATE TABLE criptos_price")

#Insertar los datos en la base de datos
execute_values(cur, insert_query, tuplas)

# Confirmar los cambios
conn.commit()

#Verifico los datos cargados en RedShift mediante consulta
query_db = "SELECT name as cripto, priceUsd as cotizacion FROM criptos_price"
cur.execute(query_db)
columnas = [description[0] for description in cur.description]
a = cur.fetchall()
print (pd.DataFrame(a,columns=columnas))


# Cerrar conexión
cur.close()
conn.close()