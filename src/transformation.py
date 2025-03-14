import re
import logging

def extract_mentions(drugs_df, pubmed_df, clinical_trials_df):
    """
    Pour chaque publication (PubMed et essais cliniques),
    vérifie si un drug est mentionné dans le titre.
    Retourne un dictionnaire de mentions par drug.
    """
    try:
        mentions = {}  # dictionnaire : drug -> { "pubmed": [...], "clinical_trials": [...] }

        # Initialisation du dictionnaire pour chaque drug
        for _, row in drugs_df.iterrows():
            drug_name = row['drug']
            mentions[drug_name] = {"pubmed": [], "clinical_trials": []}
        
        # Traitement des publications PubMed
        for _, row in pubmed_df.iterrows():
            title = row['title']
            for _, drug_row in drugs_df.iterrows():
                drug_name = drug_row['drug']
                # Recherche insensible à la casse avec mot entier
                if re.search(r'\b' + re.escape(drug_name) + r'\b', title, re.IGNORECASE):
                    mention = {
                        "id": row['id'],
                        "title": title,
                        "journal": row['journal'],
                        "date": row['date']
                    }
                    mentions[drug_name]["pubmed"].append(mention)
                    
        # Traitement des essais cliniques
        for _, row in clinical_trials_df.iterrows():
            title = row['scientific_title']
            for _, drug_row in drugs_df.iterrows():
                drug_name = drug_row['drug']
                if re.search(r'\b' + re.escape(drug_name) + r'\b', title, re.IGNORECASE):
                    mention = {
                        "id": row['id'],
                        "title": title,
                        "journal": row['journal'],
                        "date": row['date']
                    }
                    mentions[drug_name]["clinical_trials"].append(mention)
        logging.info("Extraction des mentions terminée avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des mentions: {e}")
        raise
    
    return mentions
def build_graph(drugs_df, mentions):
    """
    Construit un graph sous forme de dictionnaire.
    Chaque drug contient ses mentions et la liste des journaux ayant publié ces mentions.
    """
    try:
        graph = {"drugs": []}

        for _, drug_row in drugs_df.iterrows():
            drug_name = drug_row['drug']
            drug_node = {
                "atccode": drug_row['atccode'],
                "drug": drug_name,
                "mentions": mentions.get(drug_name, {}),
            }

            # Initialisation d'un dictionnaire pour stocker les journaux et dates
            journals = {}

            # Remplissage des journaux à partir des mentions
            for source in ["pubmed", "clinical_trials"]:
                for mention in mentions.get(drug_name, {}).get(source, []):
                    journal = mention["journal"]
                    date = mention["date"]

                    # Ajout du journal et des dates sans doublons
                    if journal not in journals:
                        journals[journal] = set()  # set pour éviter les doublons
                    journals[journal].add(date)

            # Conversion du dictionnaire de journaux en liste de dicts avec les dates triées
            drug_node["journals"] = [
                {"title": journal, "dates": sorted(list(dates))}
                for journal, dates in journals.items()
            ]
            
            # Ajout des journaux à mentions (sans doublons)
            drug_node["mentions"]["journals"] = drug_node["journals"]

            # Ajouter le nœud drug au graph
            graph["drugs"].append(drug_node)
        
        logging.info("Construction du graphe terminée avec succès.")

    except Exception as e:
        logging.error(f"Erreur lors de la construction du graphe: {e}")
        raise
    return graph
