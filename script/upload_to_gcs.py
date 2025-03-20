from google.cloud import storage
import os
import json
import logging

# Fonction pour télécharger un dossier entier vers Google Cloud Storage
def upload_folder_to_gcs(bucket_name, source_folder, destination_folder):
    try:
        # Création d'un client pour interagir avec GCS
        client = storage.Client()
        # Récupération du bucket spécifié
        bucket = client.get_bucket(bucket_name)
        
        # Parcours récursif du dossier source
        for root, _, files in os.walk(source_folder):
            for file in files:
                # Construction du chemin local et du chemin dans le bucket
                local_path = os.path.join(root, file)
                blob_path = os.path.join(destination_folder, os.path.relpath(local_path, source_folder))
                # Création d'un blob pour le fichier et téléchargement vers GCS
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(local_path)
                logging.info(f"Téléchargement de {local_path} vers gs://{bucket_name}/{blob_path} terminé.")
    except Exception as e:
        # Gestion des erreurs lors du téléchargement
        logging.error(f"Erreur lors de l'upload du dossier {source_folder} vers {destination_folder}: {e}")
        raise

# Charger le fichier de configuration pour obtenir le nom du bucket
with open('data/composer_bucket.json') as f:
    data = json.load(f)
    bucket_name = data['composer_bucket'].replace('gs://', '')

# Télécharger les dossiers DAGs et src vers le répertoire 'dags' dans GCS
upload_folder_to_gcs(bucket_name, 'dags', 'dags')
upload_folder_to_gcs(bucket_name, 'src', 'dags/src')

# Télécharger les données vers le répertoire 'data' dans GCS
upload_folder_to_gcs(bucket_name, 'data', 'data')
