import pandas as pd
import logging
from google.cloud import storage
import io
import json


def load_data():
    """
    Load data from Google Cloud Storage.
    """
    try:
        # Configuration pour lire les fichiers depuis Google Cloud Storage
        with open('/home/airflow/gcs/data/composer_bucket.json') as f:
            data = json.load(f)
            bucket_name = data['composer_bucket'].replace('gs://', '')
        bucket = storage.Client().get_bucket(bucket_name)
        
        # Chemin vers les fichiers CSV
        drugs_file_path = 'data/drugs.csv'
        pubmed_csv_file_path = 'data/pubmed.csv'
        clinical_trials_file_path = 'data/clinical_trials.csv'
        pubmed_json_file_path = 'data/pubmed.json'
        
        # Lire les fichiers CSV depuis GCS
        drugs_blob = bucket.blob(drugs_file_path)
        drugs_data = drugs_blob.download_as_text()
        drugs_df = pd.read_csv(io.StringIO(drugs_data))
        
        pubmed_csv_blob = bucket.blob(pubmed_csv_file_path)
        pubmed_csv_data = pubmed_csv_blob.download_as_text()
        pubmed_csv_df = pd.read_csv(io.StringIO(pubmed_csv_data))
        
        clinical_trials_blob = bucket.blob(clinical_trials_file_path)
        clinical_trials_data = clinical_trials_blob.download_as_text()
        clinical_trials_df = pd.read_csv(io.StringIO(clinical_trials_data))
        
        pubmed_json_blob = bucket.blob(pubmed_json_file_path)
        pubmed_json_data = pubmed_json_blob.download_as_text()
        pubmed_json_df = pd.read_json(io.StringIO(pubmed_json_data))

        # Uniformiser le format des dates
        pubmed_csv_df['date'] = pd.to_datetime(pubmed_csv_df['date'], format='mixed', dayfirst=True, errors='coerce')
        pubmed_json_df['date'] = pd.to_datetime(pubmed_json_df['date'], format='mixed', dayfirst=True, errors='coerce')
        clinical_trials_df['date'] = pd.to_datetime(clinical_trials_df['date'], format='mixed', dayfirst=True, errors='coerce')

        logging.info("Chargement des données réussi.")
        return drugs_df, pubmed_csv_df, pubmed_json_df, clinical_trials_df
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données: {e}")
        raise
