from google.cloud import storage
import json
import logging
from collections import defaultdict

def read_json_from_gcs(bucket_name, blob_name):
    """Reads a JSON file from GCS and returns its content as a dictionary."""
    try:
        # Initialize a storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the contents of the blob as a string
        json_data = blob.download_as_text()
        
        # Parse the JSON data
        data = json.loads(json_data)
        return data
    except Exception as e:
        logging.error("Erreur lors de la lecture du JSON depuis GCS: %s", e)
        raise

def get_journal_with_most_drugs(bucket_name, blob_name):
    """Extracts the journal(s) with the most drugs mentioned from a JSON file in GCS."""
    try:
        # Read the JSON data from GCS
        data = read_json_from_gcs(bucket_name, blob_name)
        
        # Initialize defaultdict to count drugs for each journal
        journal_drug_count = defaultdict(set)
        
        # Process the data as before
        for drug in data.get("drugs", []):
            for journal_entry in drug.get("mentions", {}).get("journals", []):
                journal = journal_entry.get("title")
                if journal:
                    journal_drug_count[journal].add(drug["drug"])
        
        # Find the maximum number of drugs mentioned by any journal
        max_count = 0
        for drugs in journal_drug_count.values():
            max_count = max(max_count, len(drugs))
        
        # Collect all journals with the maximum count of drugs
        max_journals = [journal for journal, drugs in journal_drug_count.items() if len(drugs) == max_count]
        
        if max_journals:
            logging.info("Les journaux avec le plus de médicaments mentionnés (%d médicaments) sont: %s", max_count, ", ".join(max_journals))
        else:
            logging.info("Aucun journal n'a été trouvé.")
        
        return max_journals
    except Exception as e:
        logging.error("Erreur lors de l'analyse du JSON: %s", e)
        raise

# Example usage
if __name__ == "__main__":
    with open('data/composer_bucket.json') as f:
        data = json.load(f)
        bucket_name = data['composer_bucket'].replace('gs://', '')
    
    journals = get_journal_with_most_drugs(bucket_name, "data/output.json")
    if journals:
        print("Journaux avec le plus de médicaments différents:", ", ".join(journals))
    else:
        print("Aucun journal avec des médicaments n'a été trouvé.")
