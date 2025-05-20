import json
import csv
import logging

def load_flights(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]
        logging.info(f"{len(data)} lignes chargées depuis le fichier '{filepath}'.")
        return data
    except Exception as e:
        logging.error(f"Erreur lors du chargement du fichier '{filepath}': {e}")
        return []

def load_csv(filepath):
    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = [row for row in reader]
        logging.info(f"{len(rows)} lignes chargées depuis le CSV '{filepath}'.")
        return rows
    except Exception as e:
        logging.error(f"Erreur lors du chargement du CSV '{filepath}': {e}")
        return []
