import logging

def reset_flights_table(cur, conn):
    logging.info("Suppression de la table 'flights' si elle existe...")
    cur.execute("DROP TABLE IF EXISTS flights")
    conn.commit()

    logging.info("Création de la table 'flights'...")
    cur.execute("""
        CREATE TABLE flights (
            id SERIAL PRIMARY KEY,
            fl_date DATE,
            dep_delay INT,
            arr_delay INT,
            air_time INT,
            distance INT,
            dep_time FLOAT,
            arr_time FLOAT
        )
    """)
    conn.commit()

def add_or_rename_destination_column(cur, conn):
    logging.info("Vérification et modification de la colonne 'destination' dans 'flights'...")

    # Supprimer 'destination' si elle existe déjà (pour repartir propre)
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='flights' AND column_name='destination'
            ) THEN
                ALTER TABLE flights DROP COLUMN destination;
            END IF;
        END
        $$;
    """)
    conn.commit()

    # Si 'groupe' existe, renommer en 'destination', sinon créer 'destination'
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name='flights' AND column_name='groupe'
    """)
    if cur.fetchone():
        logging.info("Renommage de la colonne 'groupe' en 'destination'...")
        cur.execute("ALTER TABLE flights RENAME COLUMN groupe TO destination;")
    else:
        logging.info("Création de la colonne 'destination'...")
        cur.execute("ALTER TABLE flights ADD COLUMN destination TEXT;")
    conn.commit()

    # Remplir la colonne 'destination' avec valeurs aléatoires A1 à A30
    logging.info("Remplissage de la colonne 'destination' avec A1 à A30 aléatoires...")
    cur.execute("UPDATE flights SET destination = 'A' || FLOOR(1 + RANDOM() * 30)::INT;")
    conn.commit()

def add_depart_column(cur, conn):
    logging.info("Ajout ou remplacement de la colonne 'depart'...")

    # Supprimer 'depart' si elle existe déjà
    cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='flights' AND column_name='depart'
            ) THEN
                ALTER TABLE flights DROP COLUMN depart;
            END IF;
        END
        $$;
    """)
    conn.commit()

    # Créer la colonne 'depart'
    logging.info("Création de la colonne 'depart'...")
    cur.execute("ALTER TABLE flights ADD COLUMN depart TEXT;")
    conn.commit()

    # Remplir la colonne 'depart' avec valeurs aléatoires B1 à B100
    logging.info("Remplissage de la colonne 'depart' avec B1 à B100 aléatoires...")
    cur.execute("UPDATE flights SET depart = 'B' || FLOOR(1 + RANDOM() * 100)::INT;")
    conn.commit()

def reset_depart_and_destination_tables(cur, conn):
    logging.info("Suppression des tables 'depart' et 'destination' si elles existent...")
    cur.execute("DROP TABLE IF EXISTS depart")
    cur.execute("DROP TABLE IF EXISTS destination")
    conn.commit()

    logging.info("Création de la table 'depart'...")
    cur.execute("""
        CREATE TABLE depart (
            id TEXT PRIMARY KEY,
            nom TEXT
        )
    """)

    logging.info("Création de la table 'destination'...")
    cur.execute("""
        CREATE TABLE destination (
            id TEXT PRIMARY KEY,
            nom TEXT
        )
    """)
    conn.commit()
