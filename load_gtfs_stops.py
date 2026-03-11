import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json

API_KEY = "4756c636-6b91-461d-a590-a84f17363f13"

OPERATORS_URL = "http://api.511.org/transit/gtfsoperators"
DATAFEED_URL = "http://api.511.org/transit/datafeeds"

DB_CONFIG = {
    "host": "postgres.railway.internal",
    "database": "railway",
    "user": "postgres",
    "password": "JEROCknTYsMjelLkphuPjIcygiEBlliJ",
    "port": 5432
}


def get_operators():
    params = {
        "api_key": API_KEY
    }

    r = requests.get(OPERATORS_URL, params=params)
    r.raise_for_status()

    data = json.loads(r.content.decode("utf-8-sig"))

    operators = [op["Id"] for op in data]

    return operators


def download_feed(operator_id):

    params = {
        "api_key": API_KEY,
        "operator_id": operator_id
    }

    r = requests.get(DATAFEED_URL, params=params)
    r.raise_for_status()

    return r.content


def load_stops_from_zip(zip_bytes, operator_id):

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:

        if "stops.txt" not in z.namelist():
            return []

        df = pd.read_csv(z.open("stops.txt"))

        df["operator_id"] = operator_id

        return df[["operator_id", "stop_id", "stop_name", "stop_lat", "stop_lon"]]


def insert_stops(df, conn):

    with conn.cursor() as cur:

        for _, row in df.iterrows():

            cur.execute(
                """
                INSERT INTO stops (operator_id, stop_id, stop_name, stop_lat, stop_lon)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    row.operator_id,
                    row.stop_id,
                    row.stop_name,
                    row.stop_lat,
                    row.stop_lon
                )
            )

    conn.commit()


def run():

    conn = psycopg2.connect(**DB_CONFIG)

    operators = get_operators()

    print("Operators:", operators)

    for operator_id in operators:

        print("Downloading", operator_id)

        zip_data = download_feed(operator_id)

        df = load_stops_from_zip(zip_data, operator_id)

        if len(df) > 0:
            insert_stops(df, conn)

    conn.close()