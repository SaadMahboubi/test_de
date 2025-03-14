import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from transformation import extract_mentions, build_graph

class TestTransformation(unittest.TestCase):

    @patch('transformation.logging.info')
    def test_extract_mentions_successful(self, mock_logging_info):
        drugs_df = pd.DataFrame({
            'drug': ['Paracetamol', 'Ibuprofen']
        })
        pubmed_df = pd.DataFrame({
            'id': [1, 2],
            'title': ['Study of Paracetamol effectiveness', 'Ibuprofen and its uses'],
            'journal': ['Journal A', 'Journal B'],
            'date': ['2025-03-13', '2025-03-14']
        })
        clinical_trials_df = pd.DataFrame({
            'id': [3],
            'scientific_title': ['Clinical study on Ibuprofen'],
            'journal': ['Journal C'],
            'date': ['2025-03-15']
        })

        mentions = extract_mentions(drugs_df, pubmed_df, clinical_trials_df)

        self.assertIn('Paracetamol', mentions)
        self.assertIn('Ibuprofen', mentions)

        self.assertEqual(len(mentions['Paracetamol']['pubmed']), 1)
        self.assertEqual(len(mentions['Ibuprofen']['pubmed']), 1)
        self.assertEqual(len(mentions['Ibuprofen']['clinical_trials']), 1)

        mock_logging_info.assert_called_with("Extraction des mentions terminée avec succès.")

    @patch('transformation.logging.error')
    def test_extract_mentions_failure(self, mock_logging_error):
        with self.assertRaises(Exception):
            extract_mentions(None, None, None)

        mock_logging_error.assert_called()

    @patch('transformation.logging.info')
    def test_build_graph_successful(self, mock_logging_info):
        drugs_df = pd.DataFrame({
            'atccode': ['A01', 'B02'],
            'drug': ['Paracetamol', 'Ibuprofen']
        })

        # Modification des données de test
        mentions = {
            'Paracetamol': {
                'pubmed': [{'id': 1, 'title': 'Study of Paracetamol effectiveness', 'date': '2025-03-13', 'journal': 'Journal A'}],
                'clinical_trials': [],
                'journals': [{'title': 'Journal A', 'dates': ['2025-03-13']}]
            },
            'Ibuprofen': {
                'pubmed': [{'id': 2, 'title': 'Ibuprofen and its uses', 'date': '2025-03-14', 'journal': 'Journal B'}],
                'clinical_trials': [{'id': 3, 'title': 'Clinical study on Ibuprofen', 'date': '2025-03-15', 'journal': 'Journal C'}],
                'journals': [{'title': 'Journal B', 'dates': ['2025-03-14']}, {'title': 'Journal C', 'dates': ['2025-03-15']}]
            }
        }

        graph = build_graph(drugs_df, mentions)

        self.assertEqual(len(graph['drugs']), 2)
        paracetamol_node = next((drug for drug in graph['drugs'] if drug['drug'] == 'Paracetamol'), None)
        ibuprofen_node = next((drug for drug in graph['drugs'] if drug['drug'] == 'Ibuprofen'), None)

        self.assertIsNotNone(paracetamol_node)
        self.assertIsNotNone(ibuprofen_node)

        self.assertEqual(len(paracetamol_node['mentions']['pubmed']), 1)
        self.assertEqual(len(paracetamol_node['mentions']['journals']), 1)
        
        # Vérification de la structure des journaux dans `mentions`
        self.assertEqual(paracetamol_node['mentions']['journals'][0]['title'], 'Journal A')

        self.assertEqual(len(ibuprofen_node['mentions']['clinical_trials']), 1)
        self.assertEqual(len(ibuprofen_node['mentions']['journals']), 2)

        # Vérifier les titres des journaux dans ibuprofen_node
        journal_titles = [journal['title'] for journal in ibuprofen_node['mentions']['journals']]
        self.assertIn('Journal B', journal_titles)
        self.assertIn('Journal C', journal_titles)

        mock_logging_info.assert_called_with("Construction du graphe terminée avec succès.")

    @patch('transformation.logging.error')
    def test_build_graph_failure(self, mock_logging_error):
        with self.assertRaises(Exception):
            build_graph(None, None)

        mock_logging_error.assert_called()

if __name__ == '__main__':
    unittest.main()
