import unittest
from unittest.mock import patch, MagicMock
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from export import export_json_to_gcs
import pandas as pd
import json
import logging
# Charger le fichier de configuration pour obtenir le nom du bucket
with open('data/composer_bucket.json') as f:
    data_bucket = json.load(f)
    bucket_name = data_bucket['composer_bucket']


class TestExportJsonToGCS(unittest.TestCase):

    # Ce test vérifie que l'exportation JSON vers GCS réussit lorsque les données sont correctement formatées.
    # Il simule les interactions avec le client de stockage et vérifie que les appels attendus sont effectués.
    @patch('export.logging.info')
    @patch('export.storage.Client')
    def test_export_successful(self, mock_storage_client, mock_logging_info):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        data = {"key": "value", "timestamp": pd.Timestamp('2025-03-13T12:00:00')}
        destination_blob_name = "test.json"

        export_json_to_gcs(data, bucket_name, destination_blob_name)

        mock_storage_client.return_value.get_bucket.assert_called_with(bucket_name)
        mock_bucket.blob.assert_called_with(destination_blob_name)

        expected_json = json.dumps(
            data,
            ensure_ascii=False,
            indent=4,
            default=lambda o: o.isoformat() if isinstance(o, pd.Timestamp) else None
        )

        mock_blob.upload_from_string.assert_called_with(
            expected_json,
            content_type='application/json'
        )

        mock_logging_info.assert_called_with(
            "Fichier JSON exporté vers GCS: gs://%s/%s", bucket_name, destination_blob_name
        )

    # Ce test vérifie que l'erreur de sérialisation JSON est correctement levée lorsque les données ne sont pas sérialisables.
    @patch('export.storage.Client')
    def test_export_json_serialization_error(self, mock_storage_client):
        mock_storage_client.return_value.get_bucket.side_effect = Exception("Test Exception")

        data = {"key": set([1, 2, 3])} 
        destination_blob_name = "test.json"

        with self.assertRaises(TypeError):
            export_json_to_gcs(data, bucket_name, destination_blob_name)

    # Ce test vérifie que l'erreur d'upload GCS est correctement gérée lorsque l'upload échoue.
    @patch('export.storage.Client')
    def test_export_gcs_upload_failure(self, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        mock_blob.upload_from_string.side_effect = Exception("Upload error")

        data = {"key": "value"}
        destination_blob_name = "test.json"

        with self.assertRaises(Exception):
            export_json_to_gcs(data, bucket_name, destination_blob_name)


if __name__ == '__main__':
    unittest.main()
