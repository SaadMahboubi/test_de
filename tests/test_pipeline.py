import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from pipeline import main
import pandas as pd
import json
import logging

# Charger le fichier de configuration pour obtenir le nom du bucket
with open('data/composer_bucket.json') as f:
    data_bucket = json.load(f)
    bucket_name = data_bucket['composer_bucket']

class TestPipelineMain(unittest.TestCase):

    @patch('pipeline.export_json_to_gcs')
    @patch('pipeline.build_graph')
    @patch('pipeline.extract_mentions')
    @patch('pipeline.load_data')
    @patch('builtins.open', new_callable=mock_open, read_data='{"composer_bucket": "' + bucket_name + '"}')
    @patch('pipeline.logging.info')
    def test_main_successful(self, mock_logging_info, mock_file, mock_load_data, mock_extract_mentions, mock_build_graph, mock_export_json_to_gcs):

        # Mock load_data
        mock_load_data.return_value = (
            pd.DataFrame({'drug_id': [1], 'drug_name': ['Paracetamol']}),
            pd.DataFrame({'id': [1], 'date': ['2025-03-13']}),
            pd.DataFrame({'id': [2], 'date': ['2025-03-14']}),
            pd.DataFrame({'id': [3], 'date': ['2025-03-15']})
        )

        # Mock extract_mentions
        mock_extract_mentions.return_value = pd.DataFrame({'drug_id': [1], 'mention': ['Mention test']})

        # Mock build_graph
        mock_build_graph.return_value = {'graph': 'data'}

        main()

        mock_load_data.assert_called_once()
        mock_extract_mentions.assert_called_once()
        mock_build_graph.assert_called_once()
        mock_export_json_to_gcs.assert_called_once_with({'graph': 'data'}, bucket_name, "data/output.json")
        mock_logging_info.assert_any_call("Démarrage de la data pipeline.")
        mock_logging_info.assert_any_call("Data pipeline terminée avec succès.")

    @patch('pipeline.load_data')
    @patch('pipeline.logging.error')
    def test_main_failure_load_data(self, mock_logging_error, mock_load_data):
        mock_load_data.side_effect = Exception("Load data error")

        with self.assertRaises(Exception):
            main()

        mock_logging_error.assert_called_with("Erreur lors de l'exécution de la pipeline: Load data error")

if __name__ == '__main__':
    unittest.main()
