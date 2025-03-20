import logging
import pandas as pd
from ingestion import load_data
from transformation import extract_mentions, build_graph
from export import export_json_to_gcs
import json

def main():
    try:
        logging.basicConfig(level=logging.INFO)
        logging.info("Démarrage de la data pipeline.")

        # Ingestion des données
        drugs_df, pubmed_csv_df, pubmed_json_df, clinical_trials_df = load_data()

        # Fusionner les publications PubMed provenant du CSV et du JSON
        pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df], ignore_index=True)
        
        # Extraction des mentions de drugs dans les publications
        mentions = extract_mentions(drugs_df, pubmed_df, clinical_trials_df)
        
        # Construction du graphe de liaison
        graph = build_graph(drugs_df, mentions)
        # Charger le fichier de configuration pour obtenir le nom du bucket
        with open('/home/airflow/gcs/data/composer_bucket.json') as f:
            data = json.load(f)
            bucket_name = data['composer_bucket'].replace('gs://', '')
        # Export du graphe en JSON vers GCS
        export_json_to_gcs(graph, bucket_name, "data/output.json")
        
        logging.info("Data pipeline terminée avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'exécution de la pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
