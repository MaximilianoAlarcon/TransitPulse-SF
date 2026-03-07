import requests
import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime
import os

#http://api.511.org/transit/VehicleMonitoring?api_key=4756c636-6b91-461d-a590-a84f17363f13&agency=AC"
POLL_INTERVAL = 30

# --- Configuración ---
API_URL = os.getenv("VEHICLE_API_URL")  # Endpoint real-time vehicle monitoring
POLL_INTERVAL = 30  # segundos

DB_HOST = os.getenv("PGHOST")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# --- Conexión a la base de datos ---
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# --- Inicializar la tabla ---
def init_db():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_positions (
        vehicle_id TEXT,
        recorded_at TIMESTAMP,
        line_ref TEXT,
        direction TEXT,
        origin_name TEXT,
        destination_name TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        bearing DOUBLE PRECISION,
        occupancy TEXT,
        operator TEXT,
        PRIMARY KEY (vehicle_id, recorded_at)
    )
    """)
    conn.commit()
    conn.close()


# --- Obtener datos de la API ---
def fetch_vehicle_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print("Error fetching API:", e)
        return []


# --- Parsear JSON según la estructura que me pasaste ---
def parse_vehicle_data(data):
    vehicles = []
    for v in data:  # cada elemento es un vehículo
        journey = v.get("MonitoredVehicleJourney", {})
        location = journey.get("VehicleLocation", {})

        vehicles.append({
            "vehicle_id": journey.get("VehicleRef"),
            "recorded_at": v.get("RecordedAtTime"),
            "line_ref": journey.get("LineRef"),
            "direction": journey.get("DirectionRef"),
            "origin_name": journey.get("OriginName"),
            "destination_name": journey.get("DestinationName"),
            "latitude": float(location.get("Latitude", 0)),
            "longitude": float(location.get("Longitude", 0)),
            "bearing": float(journey.get("Bearing", 0)),
            "occupancy": journey.get("Occupancy"),
            "operator": journey.get("OperatorRef")
        })
    return vehicles


# --- Insertar en la DB evitando duplicados ---
def insert_data(vehicles):
    if not vehicles:
        return
    conn = connect_db()
    cursor = conn.cursor()

    values = [
        (
            v["vehicle_id"],
            v["recorded_at"],
            v["line_ref"],
            v["direction"],
            v["origin_name"],
            v["destination_name"],
            v["latitude"],
            v["longitude"],
            v["bearing"],
            v["occupancy"],
            v["operator"]
        )
        for v in vehicles
    ]

    sql = """
    INSERT INTO vehicle_positions (
        vehicle_id, recorded_at, line_ref, direction,
        origin_name, destination_name, latitude, longitude,
        bearing, occupancy, operator
    ) VALUES %s
    ON CONFLICT (vehicle_id, recorded_at) DO NOTHING
    """

    try:
        execute_values(cursor, sql, values)
        conn.commit()
        print(f"[{datetime.utcnow()}] Inserted {len(values)} vehicles")
    except Exception as e:
        print("Error inserting into DB:", e)
    finally:
        conn.close()


# --- Pipeline principal ---
def run_pipeline():
    init_db()
    while True:
        data = fetch_vehicle_data()
        vehicles = parse_vehicle_data(data)
        insert_data(vehicles)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run_pipeline()