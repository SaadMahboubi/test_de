import unittest
from unittest.mock import patch, MagicMock
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../script')))

from upload_to_gcs import upload_folder_to_gcs
import json

# Charger le fichier de configuration pour obtenir le nom du bucket
with open('data/composer_bucket.json') as f:
    data = json.load(f)
    bucket_name = data['composer_bucket'].replace('gs://', '')

class TestUploadToGCS(unittest.TestCase):

    # Ce test vérifie que le dossier est correctement téléchargé vers GCS lorsque le client de stockage fonctionne comme prévu.
    # Il simule les interactions avec le client de stockage et le système de fichiers pour vérifier que tous les fichiers sont téléchargés.
    @patch('upload_to_gcs.logging.info')
    @patch('upload_to_gcs.storage.Client')
    @patch('os.walk')
    def test_upload_folder_to_gcs_successful(self, mock_os_walk, mock_storage_client, mock_logging_info):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        mock_os_walk.return_value = [
            ('source_folder', ('subdir',), ('file1.txt', 'file2.txt')),
            ('source_folder/subdir', (), ('file3.txt',))
        ]

        upload_folder_to_gcs(bucket_name, 'source_folder', 'destination_folder')

        self.assertEqual(mock_blob.upload_from_filename.call_count, 3)
        mock_logging_info.assert_called_with(f"Téléchargement de source_folder/subdir/file3.txt vers gs://{bucket_name}/destination_folder/subdir/file3.txt terminé.")

    # Ce test vérifie que l'erreur est correctement levée et enregistrée lorsque le client de stockage rencontre une exception.
    @patch('upload_to_gcs.logging.error')
    @patch('upload_to_gcs.storage.Client')
    def test_upload_folder_to_gcs_failure(self, mock_storage_client, mock_logging_error):
        mock_storage_client.return_value.get_bucket.side_effect = Exception("Test Exception")

        with self.assertRaises(Exception):
            upload_folder_to_gcs(bucket_name, 'source_folder', 'destination_folder')

        mock_logging_error.assert_called_with(
            "Erreur lors de l'upload du dossier source_folder vers destination_folder: Test Exception"
        )

if __name__ == '__main__':
    unittest.main()
