from google.cloud import storage
import json
import logging
from collections import defaultdict

# Fonction pour lire un fichier JSON depuis GCS et retourner son contenu sous forme de dictionnaire
def read_json_from_gcs(bucket_name, blob_name):
    """Lit un fichier JSON depuis GCS et renvoie son contenu sous forme de dictionnaire."""
    try:
        # Création d'un client pour interagir avec GCS
        storage_client = storage.Client()
        # Récupération du bucket et du blob spécifiés
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Téléchargement du contenu du blob sous forme de texte
        json_data = blob.download_as_text()
        
        # Parsing du contenu JSON en dictionnaire
        data = json.loads(json_data)
        return data
    except Exception as e:
        # Gestion des erreurs lors de la lecture du JSON depuis GCS
        logging.error("Erreur lors de la lecture du JSON depuis GCS: %s", e)
        raise

# Fonction pour extraire le(s) journal(aux) avec le plus de médicaments mentionnés à partir d'un fichier JSON dans GCS
def get_journal_with_most_drugs(bucket_name, blob_name):
    """Extrait le(s) journal(s) avec le plus de médicaments mentionnés à partir d'un fichier JSON dans GCS."""
    try:
        # Lecture du contenu du fichier JSON depuis GCS
        data = read_json_from_gcs(bucket_name, blob_name)
        
        # Création d'un dictionnaire pour stocker le nombre de médicaments par journal
        journal_drug_count = defaultdict(set)
        
        # Itération sur les médicaments et leurs mentions dans les journaux
        for drug in data.get("drugs", []):
            for journal_entry in drug.get("mentions", {}).get("journals", []):
                # Récupération du titre du journal
                journal = journal_entry.get("title")
                if journal:
                    # Ajout du médicament au journal correspondant
                    journal_drug_count[journal].add(drug["drug"])
        
        # Recherche du nombre maximum de médicaments par journal
        max_count = 0
        for drugs in journal_drug_count.values():
            max_count = max(max_count, len(drugs))
        
        # Recherche des journaux avec le nombre maximum de médicaments
        max_journals = [journal for journal, drugs in journal_drug_count.items() if len(drugs) == max_count]
        
        # Affichage des résultats
        if max_journals:
            logging.info("Les journaux avec le plus de médicaments mentionnés (%d médicaments) sont: %s", max_count, ", ".join(max_journals))
        else:
            logging.info("Aucun journal n'a été trouvé.")
        
        return max_journals
    except Exception as e:
        # Gestion des erreurs lors de l'analyse du JSON
        logging.error("Erreur lors de l'analyse du JSON: %s", e)
        raise

if __name__ == "__main__":
    # Charger le fichier de configuration pour obtenir le nom du bucket
    with open('data/composer_bucket.json') as f:
        data = json.load(f)
        bucket_name = data['composer_bucket'].replace('gs://', '')
    
    # Appeler la fonction pour obtenir les journaux avec le plus de médicaments mentionnés
    journals = get_journal_with_most_drugs(bucket_name, "data/output.json")
    if journals:
        print("Journaux avec le plus de médicaments différents:", ", ".join(journals))
    else:
        print("Aucun journal avec des médicaments n'a été trouvé.")
