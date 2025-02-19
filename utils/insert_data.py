import psycopg2
import csv
import os

TABLE_NAME = "inflation_data"

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("Erreur : La variable d'environnement DATABASE_URL n'est pas définie.")

CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    Country VARCHAR(100),
    Country_Code VARCHAR(10),
    Year INTEGER,
    Inflation FLOAT
);
"""

TABLE_COLUMNS = ["Country", "Country_Code", "Year", "Inflation"]

INSERT_QUERY = f"""
    INSERT INTO {TABLE_NAME} ({', '.join(TABLE_COLUMNS)})
    VALUES ({', '.join(['%s' for _ in TABLE_COLUMNS])})
    ON CONFLICT DO NOTHING;
"""

def get_conn():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        exit(1)

def create_table(conn):
    with conn.cursor() as cur:
        try:
            cur.execute(CREATE_TABLE_QUERY)
            conn.commit()
            print("Table créée avec succès (ou déjà existante).")
        except Exception as e:
            print(f"Erreur lors de la création de la table : {e}")

def insert_data(conn, input_file_path):
    with conn.cursor() as cursor, open(input_file_path, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)

        for row in csv_reader:
            try:
                cursor.execute(INSERT_QUERY, row)
            except Exception as e:
                print(f"Erreur lors de l'insertion de la ligne {row} : {e}")

        conn.commit()
    print(f"Données chargées avec succès depuis {input_file_path}.")

if __name__ == "__main__":
    conn = get_conn()
    try:
        create_table(conn)
        insert_data(conn, "data/inflation_data.csv")
    finally:
        conn.close()
        print("Connexion fermée.")
