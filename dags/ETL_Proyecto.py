import json
import os
import requests
import pandas as pd
#from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#Defino Querys para insertar o consultar como constante
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
query_db = "SELECT name as cripto, priceUsd as cotizacion FROM criptos_price"

dag_path = os.getcwd()

default_args = {
    'start_date': datetime(2024, 4, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ingestion_dag = DAG(
    dag_id='API_CoinCap_ETL',
    default_args=default_args,
    description='Extrae informacion actual del mercado de criptomonedas',
     schedule_interval="@daily",
    catchup=False
)

def get_data_api():
    #Realiza la solicitud a la API
    response = requests.get('https://api.coincap.io/v2/assets')

    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
         # Guardo en DataFrame los datos de la API
         df_coincap = response.json() 
    else:
        # Si la solicitud falla, imprime un mensaje de error
        print("Error al obtener datos de la API:", response.status_code)

    #Normalizar data frame
    df_coincap = pd.json_normalize(df_coincap, record_path =['data'], meta=['timestamp'])
    
    # Limpieza de datos
    df_coincap = df_coincap.drop(columns=['rank'])
    df_coincap['timestamp'] = pd.to_datetime(df_coincap['timestamp'])
    
    #Ordeno por precio en el mercado
    df_coincap.sort_values(by='priceUsd',ascending=False)
    
    return df_coincap

def load_data_api():
    #Conexion a DB
    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname='data-engineer-database',
            user='jorge_cardozo_coderhouse',
            password='6N05Yj0qrO',
            port='5439'
        )
        #En el caso de ser exitosa la conexion
        print("Conectado a Redshift")
    except Exception as e:
        #Si fallara la conexion nos devolveria el problema
        print("Error de conexion a Redshift")
        print(e)
    
    cur = conn.cursor()

    #Conversion datadrame a tupla
    df = get_data_api()
    tuplas = [tuple(x) for x in df.to_numpy()]

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
    cur.execute(query_db)
    columnas = [description[0] for description in cur.description]
    df = cur.fetchall()
    print (pd.DataFrame(df,columns=columnas))

    # Cerrar conexión
    cur.close()
    conn.close()

task_1 = PythonOperator(
    task_id ='extraer_data',
    python_callable = get_data_api,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id = 'cargar_data',
    python_callable = load_data_api,
    dag=ingestion_dag,
)

task_1 >> task_2