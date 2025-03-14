from google.cloud import storage
import os
import json
import logging

# Configurer les logs
logging.basicConfig(level=logging.INFO)

try:
    # Charger le fichier de configuration pour obtenir le nom du bucket
    with open('data/composer_bucket.json') as f:
        data = json.load(f)
        bucket_name = data['composer_bucket'].replace('gs://', '')
except FileNotFoundError:
    logging.error("Le fichier 'composer_bucket.json' est introuvable.")
    exit(1)
except KeyError:
    logging.error("Le fichier 'composer_bucket.json' ne contient pas la clé 'composer_bucket'.")
    exit(1)

# Créer un client GCS
client = storage.Client()

# Définir les chemins des fichiers
source_blob_name = "data/output.json"
destination_file_name = "result/output.json"

try:
    # Accéder au bucket
    bucket = client.get_bucket(bucket_name)
except Exception as e:
    logging.error(f"Erreur lors de l'accès au bucket {bucket_name}: {e}")
    exit(1)

# Télécharger le fichier
try:
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logging.info(f'Downloaded {source_blob_name} from GCS bucket {bucket_name} to local file {destination_file_name}.')
except Exception as e:
    logging.error(f"Erreur lors du téléchargement du fichier {source_blob_name}: {e}")
    exit(1)
