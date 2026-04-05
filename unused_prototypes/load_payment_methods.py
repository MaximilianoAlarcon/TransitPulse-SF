import os
from pathlib import Path

import psycopg2

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CSV_PATH = Path("data/route_payment_methods.csv")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def print_query_results(cur, query: str):
    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        print("No hay resultados")
        return

    col_names = [desc[0] for desc in cur.description]
    col_widths = []

    for i, col in enumerate(col_names):
        max_len = max(len(str(row[i])) for row in rows)
        col_widths.append(max(max_len, len(col)))

    header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(col_names))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(col_names)))

    data_lines = []
    for row in rows:
        line = " | ".join(str(item).ljust(col_widths[i]) for i, item in enumerate(row))
        data_lines.append(line)

    print("\n".join([header, separator] + data_lines))


def recreate_table(cur):
    cur.execute("""
        DROP TABLE IF EXISTS route_payment_methods;

        CREATE TABLE route_payment_methods (
            agency_id TEXT,
            operator_id TEXT,
            route_short_name TEXT,
            route_long_name TEXT,
            route_type INTEGER,
            route_type_name TEXT,
            payment_method_code TEXT,
            payment_method_desc TEXT,
            fare_price TEXT,
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


def load_csv_with_copy(cur, csv_path: Path):
    with csv_path.open("r", encoding="utf-8-sig") as f:
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


def load_route_payment_methods_to_postgres(csv_path: Path):
    if not csv_path.exists():
        return {
            "success": False,
            "error": f"CSV no encontrado: {csv_path}"
        }

    conn = None

    try:
        conn = get_connection()
        conn.autocommit = False

        with conn.cursor() as cur:
            recreate_table(cur)
            load_csv_with_copy(cur, csv_path)

            cur.execute("SELECT COUNT(*) FROM route_payment_methods;")
            inserted_rows = cur.fetchone()[0]

            print_query_results(cur, "SELECT * FROM route_payment_methods LIMIT 5;")

        conn.commit()

        return {
            "success": True,
            "message": "CSV loaded successfully into route_payment_methods",
            "csv_path": str(csv_path),
            "inserted_rows": inserted_rows,
        }

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "success": False,
            "error": str(e),
        }

    finally:
        if conn:
            conn.close()


def run():
    result = load_route_payment_methods_to_postgres(CSV_PATH)
    print("Result:", result)
    return result