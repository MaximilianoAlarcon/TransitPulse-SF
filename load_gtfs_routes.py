import requests
import zipfile
import io
import csv
import psycopg2
import os
import json

# Configuración de DB
DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}

# API key de 511
API_KEY = os.environ.get("API_511_KEY")

# URL para obtener operadores
OPERATORS_URL = f"http://api.511.org/transit/gtfsoperators?api_key={API_KEY}"


def get_operators():
    r = requests.get(OPERATORS_URL)
    r.raise_for_status()

    data = json.loads(r.content.decode("utf-8-sig"))

    operators = [op["Id"] for op in data]

    return operators


def run():

    # Conexión a PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()


    # 1️⃣ Obtener lista de operadores
    operators = get_operators()

    for op in operators:
        operator_id = op
        print(f"Procesando operador {operator_id}...")

        # 2️⃣ Descargar datafeed
        zip_url = f"http://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={operator_id}"
        r = requests.get(zip_url)
        if r.status_code != 200:
            print(f"Error descargando {operator_id}")
            continue

        # 3️⃣ Abrir ZIP en memoria
        z = zipfile.ZipFile(io.BytesIO(r.content))

        if "routes.txt" not in z.namelist():
            print(f"No hay routes.txt para {operator_id}")
            continue

        # 4️⃣ Leer CSV y hacer insert en la DB
        with z.open("routes.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                cur.execute("""
                    INSERT INTO routes(operator_id, route_id, agency_id, route_short_name, route_long_name,
                                    route_desc, route_type, route_url, route_color, route_text_color)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (operator_id, route_id) DO NOTHING;
                """, (
                    operator_id,
                    row.get("route_id"),
                    row.get("agency_id"),
                    row.get("route_short_name"),
                    row.get("route_long_name"),
                    row.get("route_desc"),
                    int(row["route_type"]) if row.get("route_type") else None,
                    row.get("route_url"),
                    row.get("route_color"),
                    row.get("route_text_color")
                ))
        conn.commit()
        print(f"Operador {operator_id} procesado correctamente.")

    cur.close()
    conn.close()
    print("Carga completa de routes.txt finalizada.")