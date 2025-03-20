# Importation des modules nécessaires pour la configuration du DAG
import sys
import os

# Ajouter le répertoire src au PYTHONPATH pour accéder aux modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pipeline

# Définition des arguments par défaut pour le DAG
# Ces arguments s'appliquent à toutes les tâches du DAG
# owner: propriétaire du DAG
# depends_on_past: ne dépend pas des exécutions passées
# start_date: date de début du DAG
# retries: nombre de tentatives de réexécution en cas d'échec
# retry_delay: délai entre les tentatives de réexécution
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instanciation du DAG avec ses paramètres
# dag_id: identifiant unique du DAG
# description: description du DAG
# schedule_interval: intervalle de planification du DAG
# catchup: ne pas exécuter les tâches manquées
dag = DAG(
    'data_pipeline_drugs',
    default_args=default_args,
    description='Pipeline de traitement des données de drugs et publications',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Fonction pour exécuter le pipeline de données
def run_pipeline():
    pipeline.main()

# Définition de la tâche PythonOperator pour exécuter la fonction run_pipeline
run_pipeline_task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

# Exécution de la tâche run_pipeline_task
run_pipeline_task
