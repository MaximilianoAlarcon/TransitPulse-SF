import requests
import zipfile
import io
import csv
import psycopg2
import os
import json

DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}

API_KEY = os.environ.get("API_511_KEY")

OPERATORS_URL = f"http://api.511.org/transit/gtfsoperators?api_key={API_KEY}"


def get_operators():
    r = requests.get(OPERATORS_URL)
    r.raise_for_status()

    data = json.loads(r.content.decode("utf-8-sig"))

    operators = [op["Id"] for op in data]

    return operators

def run():

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    operators = get_operators()

    for op in operators:

        operator_id = op
        print("Procesando operador:", operator_id)

        zip_url = f"http://api.511.org/transit/datafeeds?api_key={API_KEY}&operator_id={operator_id}"

        r = requests.get(zip_url)

        if r.status_code != 200:
            print("Error descargando", operator_id)
            continue

        z = zipfile.ZipFile(io.BytesIO(r.content))

        if "stop_times.txt" not in z.namelist():
            print("stop_times.txt no encontrado en", operator_id)
            continue

        with z.open("stop_times.txt") as f:

            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))

            header = next(reader)

            buffer = io.StringIO()

            writer = csv.writer(buffer)

            for row in reader:

                writer.writerow([operator_id] + row)

            buffer.seek(0)

            cur.copy_expert(
                """
                COPY stop_times (
                    operator_id,
                    trip_id,
                    arrival_time,
                    departure_time,
                    stop_id,
                    stop_sequence,
                    stop_headsign,
                    pickup_type,
                    drop_off_type,
                    shape_dist_traveled,
                    timepoint
                )
                FROM STDIN WITH CSV
                """,
                buffer
            )

            conn.commit()

            print("stop_times cargado para", operator_id)

    cur.close()
    conn.close()

    print("Carga completa de stop_times finalizada")