import json
import logging
from google.cloud import storage
import pandas as pd


def export_json_to_gcs(data, bucket_name, destination_blob_name):
    """
    Exporte le dictionnaire data sous forme de fichier JSON dans un bucket GCS.
    """
    try:
        # Fonction pour convertir les objets non sérialisables
        def default_converter(o):
            if isinstance(o, pd.Timestamp):
                return o.isoformat()
            raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

        # Sérialisation des données en JSON
        json_data = json.dumps(data, ensure_ascii=False, indent=4, default=default_converter)

        # Initialiser le client GCS
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Télécharger le fichier JSON dans GCS
        blob.upload_from_string(json_data, content_type='application/json')
        logging.info("Fichier JSON exporté vers GCS: gs://%s/%s", bucket_name, destination_blob_name)
    except Exception as e:
        logging.error("Erreur lors de l'export du JSON vers GCS: %s", e)
        raise
