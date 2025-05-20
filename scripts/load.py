import logging

def insert_flights(data, conn):
    try:
        cur = conn.cursor()

        for flight in data:
            cur.execute("""
                INSERT INTO flights (fl_date, dep_delay, arr_delay, air_time, distance, dep_time, arr_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                flight.get('FL_DATE'),
                flight.get('DEP_DELAY'),
                flight.get('ARR_DELAY'),
                flight.get('AIR_TIME'),
                flight.get('DISTANCE'),
                flight.get('DEP_TIME'),
                flight.get('ARR_TIME')
            ))

        conn.commit()
        logging.info("Insertion des vols terminée avec succès.")

    except Exception as e:
        logging.error(f"Erreur rencontrée pendant l'insertion des vols : {e}")
        conn.rollback()

    finally:
        if cur:
            cur.close()


def insert_csv_to_table(data, table_name, conn):
    """
    data : liste de dict, ex: [{'ID': 'A1', 'Depart': 'Paris'}, ...] ou [{'ID': 'B1', 'Destination': 'New York'}, ...]
    table_name : 'depart' ou 'destination'
    """

    try:
        cur = conn.cursor()

        # Prépare les tuples (id, nom) selon la table
        if table_name == 'depart':
            rows = [(row['ID'], row['Depart']) for row in data]
        elif table_name == 'destination':
            rows = [(row['ID'], row['Destination']) for row in data]
        else:
            raise ValueError("Table inconnue, doit être 'depart' ou 'destination'.")

        cur.executemany(
            f"INSERT INTO {table_name} (id, nom) VALUES (%s, %s)",
            rows
        )

        conn.commit()
        logging.info(f"Insertion réussie dans la table '{table_name}'.")

    except Exception as e:
        logging.error(f"Erreur lors de l'insertion dans la table '{table_name}': {e}")
        conn.rollback()

    finally:
        if cur:
            cur.close()
