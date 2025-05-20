from extract import load_flights, load_csv
from load import insert_flights, insert_csv_to_table
from scriptSQL.script_sql import reset_flights_table, add_or_rename_destination_column, add_depart_column, reset_depart_and_destination_tables
import logging
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

def main():
    try:
        logging.info("Connexion à la base de données...")
        conn = psycopg2.connect(
            host=os.getenv("DB1_HOST"),
            database=os.getenv("DB1_NAME"),
            user=os.getenv("DB1_USER"),
            password=os.getenv("DB1_PASSWORD")
        )
        cur = conn.cursor()

        # Création ou réinitialisation des tables
        reset_flights_table(cur, conn)
        reset_depart_and_destination_tables(cur, conn)

        # Chargement des CSV en mémoire (listes de dict)
        depart_data = load_csv("data/departs.csv")
        destination_data = load_csv("data/destinations.csv")

        # Insertion des données CSV dans la BDD
        insert_csv_to_table(depart_data, "depart", conn)
        insert_csv_to_table(destination_data, "destination", conn)

        # Chargement des données JSON
        logging.info("Chargement des données JSON...")
        data = load_flights("data/flights-1m.json")

        # Insertion des données JSON dans la BDD
        insert_flights(data, conn)

        # Ajout de la colonne aléatoire groupe/destination et depart
        add_or_rename_destination_column(cur, conn)
        add_depart_column(cur, conn)

        logging.info("Pipeline terminé avec succès.")

    except Exception as e:
        logging.error(f"Erreur rencontrée dans le main : {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
