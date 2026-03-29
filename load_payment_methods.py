import os
import psycopg2
import pandas as pd

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CSV_PATH = "data/route_payment_methods.csv"


def select(cur, query):
    # Ejecutar la consulta
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        print("No hay resultados")
        return

    # Obtener nombres de columnas
    col_names = [desc[0] for desc in cur.description]

    # Calcular ancho máximo de cada columna (para alinear)
    col_widths = []
    for i, col in enumerate(col_names):
        max_len = max(len(str(row[i])) for row in rows)
        col_widths.append(max(max_len, len(col)))

    # Construir la línea de encabezados
    header = " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(col_names))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(col_names)))

    # Construir las filas
    data_lines = []
    for row in rows:
        line = " | ".join(str(item).ljust(col_widths[i]) for i, item in enumerate(row))
        data_lines.append(line)

    # Combinar todo en un solo texto
    output = "\n".join([header, separator] + data_lines)
    print(output)

pd.set_option('display.max_columns', None)  # mostrar todas las columnas
pd.set_option('display.width', 200)         # ancho de la tabla en consola
pd.set_option('display.max_rows', 50)      # mostrar hasta 50 filas



def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table(cur):
    cur.execute("""

        DROP TABLE IF EXISTS route_payment_methods;

        CREATE TABLE IF NOT EXISTS route_payment_methods (
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

        select(cur, "SELECT * FROM route_payment_methods LIMIT 5")

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
    print("Result:", result)