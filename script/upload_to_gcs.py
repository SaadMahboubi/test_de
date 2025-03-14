from google.cloud import storage
import os
import json
import logging

def upload_folder_to_gcs(bucket_name, source_folder, destination_folder):
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        
        for root, _, files in os.walk(source_folder):
            for file in files:
                local_path = os.path.join(root, file)
                blob_path = os.path.join(destination_folder, os.path.relpath(local_path, source_folder))
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(local_path)
                logging.info(f"Téléchargement de {local_path} vers gs://{bucket_name}/{blob_path} terminé.")
    except Exception as e:
        logging.error(f"Erreur lors de l'upload du dossier {source_folder} vers {destination_folder}: {e}")
        raise

with open('data/composer_bucket.json') as f:
    data = json.load(f)
    bucket_name = data['composer_bucket'].replace('gs://', '')

# Upload DAGs and src to the same 'dags' directory
upload_folder_to_gcs(bucket_name, 'dags', 'dags')
upload_folder_to_gcs(bucket_name, 'src', 'dags/src')

# Upload data to the 'data' directory in GCS
upload_folder_to_gcs(bucket_name, 'data', 'data')

