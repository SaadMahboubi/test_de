import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from ingestion import load_data
import pandas as pd
import json
import logging

with open('data/composer_bucket.json') as f:
    data_bucket = json.load(f)
    bucket_name = data_bucket['composer_bucket']

class TestLoadData(unittest.TestCase):

    @patch('ingestion.logging.info')
    @patch('ingestion.storage.Client')
    @patch('builtins.open', new_callable=mock_open, read_data='{"composer_bucket": "' + bucket_name + '"}')
    def test_load_data_successful(self, mock_file, mock_storage_client, mock_logging_info):
        mock_bucket = MagicMock()
        mock_storage_client.return_value.get_bucket.return_value = mock_bucket

        # Mock blobs and their return values
        def create_mock_blob(return_value):
            mock_blob = MagicMock()
            mock_blob.download_as_text.return_value = return_value
            return mock_blob

        mock_bucket.blob.side_effect = [
            create_mock_blob("drug_id,drug_name\n1,Paracetamol"),
            create_mock_blob("id,date\n1,2025-03-13"),
            create_mock_blob("id,date\n1,2025-03-13"),
            create_mock_blob('[{"id": 1, "date": "2025-03-13"}]')
        ]

        drugs_df, pubmed_csv_df, pubmed_json_df, clinical_trials_df = load_data()

        self.assertFalse(drugs_df.empty)
        self.assertFalse(pubmed_csv_df.empty)
        self.assertFalse(pubmed_json_df.empty)
        self.assertFalse(clinical_trials_df.empty)

        mock_logging_info.assert_called_with("Chargement des données réussi.")

    @patch('ingestion.storage.Client')
    @patch('builtins.open', new_callable=mock_open, read_data='{"composer_bucket": "' + bucket_name + '"}')
    def test_load_data_failure(self, mock_file, mock_storage_client):
        mock_storage_client.return_value.get_bucket.side_effect = Exception("GCS Error")

        with self.assertRaises(Exception):
            load_data()

if __name__ == '__main__':
    unittest.main()
