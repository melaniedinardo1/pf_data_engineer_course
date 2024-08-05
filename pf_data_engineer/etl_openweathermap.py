import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar las variables de entorno del archivo .env
load_dotenv()

# Configuración de la API de OpenWeatherMap
API_KEY = os.getenv('API_KEY')
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'

# Lista de ciudades con sus coordenadas
cities = [
    {"name": "Rosario", "lat": -32.9468, "lon": -60.6393},
    {"name": "Buenos Aires", "lat": -34.6132, "lon": -58.3772},
    {"name": "Barcelona", "lat": 41.3888, "lon": 2.159},
    {"name": "New York", "lat": 43.0004, "lon": -75.4999},
    {"name": "London", "lat": 51.5085, "lon": -0.1257},
    {"name": "Paris", "lat": 48.8534, "lon": 2.3488},
    {"name": "Berlin", "lat": 52.5244, "lon": 13.4105},
    {"name": "Rome", "lat": 41.8947, "lon": 12.4839},
    {"name": "Miami", "lat": 25.7743, "lon": -80.1937},
    {"name": "Bangkok", "lat": 13.75, "lon": 100.5167}
]

# Función para extraer datos de la API
def fetch_weather_data(api_key, lat, lon):
    params = {
        'lat': lat,
        'lon': lon,
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
    # Conectar a Redshift
    conn = connect_to_redshift()

    # Crear tabla en Redshift
    create_redshift_table(conn)

    # Extraer y cargar datos para cada ciudad
    for city in cities:
        weather_data = fetch_weather_data(API_KEY, city["lat"], city["lon"])
        insert_data_to_redshift(conn, weather_data)

    # Cerrar conexión
    conn.close()

if __name__ == "__main__":
    main()