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

BATCH_SIZE = 50000


def get_operators():
    r = requests.get(OPERATORS_URL)
    r.raise_for_status()
    data = json.loads(r.content.decode("utf-8-sig"))
    return [op["Id"] for op in data]


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

            batch_count = 0
            total_count = 0

            for row in reader:

                writer.writerow([operator_id] + row)
                batch_count += 1
                total_count += 1

                if batch_count >= BATCH_SIZE:

                    buffer.seek(0)

                    cur.copy_expert("""
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
                    """, buffer)

                    conn.commit()

                    print(f"{total_count} registros cargados para {operator_id}")

                    buffer = io.StringIO()
                    writer = csv.writer(buffer)
                    batch_count = 0

            # último batch
            if batch_count > 0:

                buffer.seek(0)

                cur.copy_expert("""
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
                """, buffer)

                conn.commit()

            print(f"stop_times cargado para {operator_id} ({total_count} registros)")

    cur.close()
    conn.close()

    print("Carga completa de stop_times finalizada")