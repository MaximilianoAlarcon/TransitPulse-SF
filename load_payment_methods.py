import os
import psycopg2
from flask import jsonify

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CSV_PATH = "data/route_payment_methods.csv"


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS route_payment_methods (
            agency_id TEXT,
            operator_id TEXT,
            route_short_name TEXT,
            route_long_name TEXT,
            route_type INTEGER,
            route_type_name TEXT,
            payment_method_code TEXT,
            payment_method_desc TEXT,
            fare_price NUMERIC,
            currency TEXT,
            transfers_allowed TEXT,
            fare_media_name TEXT,
            fare_media_type TEXT,
            clipper_url TEXT,
            munimobile_url TEXT,
            fares_version TEXT,
            api_status TEXT
        );
    """)


def truncate_table(cur):
    cur.execute("TRUNCATE TABLE route_payment_methods;")


def load_csv_with_copy(cur, csv_path: str):
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        cur.copy_expert(
            """
            COPY route_payment_methods (
                agency_id,
                operator_id,
                route_short_name,
                route_long_name,
                route_type,
                route_type_name,
                payment_method_code,
                payment_method_desc,
                fare_price,
                currency,
                transfers_allowed,
                fare_media_name,
                fare_media_type,
                clipper_url,
                munimobile_url,
                fares_version,
                api_status
            )
            FROM STDIN
            WITH (
                FORMAT CSV,
                HEADER TRUE
            )
            """,
            f
        )


def load_route_payment_methods_to_postgres(csv_path: str, replace_data: bool = True):
    conn = None

    try:
        conn = get_connection()
        conn.autocommit = False

        with conn.cursor() as cur:
            create_table(cur)

            if replace_data:
                truncate_table(cur)

            load_csv_with_copy(cur, csv_path)

        conn.commit()

        return {
            "success": True,
            "message": "CSV loaded successfully into route_payment_methods",
            "csv_path": csv_path
        }

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "success": False,
            "error": str(e)
        }

    finally:
        if conn:
            conn.close()


def run():
    result = load_route_payment_methods_to_postgres(CSV_PATH, replace_data=True)
    status_code = 200 if result["success"] else 500
    return jsonify(result), status_code