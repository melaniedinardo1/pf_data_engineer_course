import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar las variables de entorno del archivo .env
load_dotenv()

# Configuración de la API de OpenWeatherMap
API_KEY = os.getenv('API_KEY')
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather?lat=-32.9468&lon=-60.6393'
LOCATION = 'Rosario'

# Función para extraer datos de la API
def fetch_weather_data(api_key, location):
    params = {
        'q': location,
        'appid': api_key,
        'units': 'metric'
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data

# Función para crear la tabla en Redshift
def create_redshift_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER IDENTITY(1,1),
        location VARCHAR(255),
        temperature FLOAT,
        weather_descriptions VARCHAR(255),
        observation_time TIMESTAMP,
        ingestion_time TIMESTAMP DEFAULT GETDATE()
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()

# Función para insertar datos en la tabla de Redshift
def insert_data_to_redshift(conn, data):
    insert_query = """
    INSERT INTO weather_data (location, temperature, weather_descriptions, observation_time)
    VALUES (%s, %s, %s, %s);
    """
    weather_descriptions = ', '.join([weather['description'] for weather in data['weather']])
    observation_time = datetime.utcfromtimestamp(data['dt'])
    with conn.cursor() as cursor:
        cursor.execute(insert_query, (
            data['name'],
            data['main']['temp'],
            weather_descriptions,
            observation_time
        ))
        conn.commit()

# Conexión a Redshift
def connect_to_redshift():
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    return conn

# Main function
def main():
    # Extraer datos de la API
    weather_data = fetch_weather_data(API_KEY, LOCATION)

    # Conectar a Redshift
    conn = connect_to_redshift()

    # Crear tabla en Redshift
    create_redshift_table(conn)

    # Insertar datos en Redshift
    insert_data_to_redshift(conn, weather_data)

    # Cerrar conexión
    conn.close()

if __name__ == "__main__":
    main()
