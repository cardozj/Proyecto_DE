import os
os.system("pip install -r requirements.txt")

import psycopg2
import requests
import json
import pandas as pd
import sqlite3
from dotenv import load_dotenv
from psycopg2 import extras

def get_data_api(url):
    # Realiza la solicitud a la API
    response = requests.get(url)
    
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
    cursor = connection.cursor()
    result = None
    try:
        #Ejecuto Query para visualizar tabla
        cursor.execute(query)
        #Guardo resultado y devuelvo a main
        result = cursor.fetchall()
        return result
    except sqlite3.Error as e:
        #En el caso de fallar, me devuelve error especifico
        print(f"Error '{e}' ha ocurrido")

# URL de la API CoinCap
url_api = "https://api.coincap.io/v2/assets"

# Obtener los datos desde la API y Normalizarlos
df_coincap = pd.json_normalize(get_data_api(url_api), record_path =['data'], meta=['timestamp'])

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
    
except Exception as e:
    #Si fallara la conexion nos devolveria el problema
    print("Error de conexion a Redshift")
    print(e)

#Envio de df
#Conversion de df a tupla
tuplas = [tuple(x) for x in df_coincap.to_numpy()]

#Conexion a db 
cursor = conn.cursor()

#Query para insertar valores
insert_query = "INSERT INTO criptos_price (id, symbol, name, supply, maxSupply, marketCapUsd, volumeUsd24Hr, priceUsd, changePercent24Hr, vwap24Hr, explorer, timestamp) VALUES %s"

#Crear la tabla si no existe
cursor.execute("""
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
cursor.execute("TRUNCATE TABLE criptos_price")

#Insertar los datos en la base de datos
extras.execute_values(cursor, insert_query, tuplas)

# Confirmar los cambios
conn.commit()

#Verifico los datos cargados en RedShift mediante consulta
query_db = "SELECT name as cripto, priceUsd as cotizacion FROM criptos_price"
cursor.execute(query_db)
columnas = [description[0] for description in cursor.description]
cursor.fetchall()
print (pd.DataFrame(execute_read_query(conn, query_db),columns=columnas))


# Cerrar conexión
cursor.close()
conn.close()