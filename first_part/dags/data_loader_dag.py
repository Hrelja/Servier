from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import json
import os

def load_csv_to_dataframe(file_path):
    """Charge un fichier CSV en DataFrame Pandas.
    
    """
    return pd.read_csv(file_path)

def save_raw_data(df, filename):
    """Sauvegarde les dataframes Pandas de la couche raw.

    """
    os.makedirs('dags/raw', exist_ok=True)  
    df.to_csv(f'dags/raw/{filename}', index=False)

def load_clinical_trials(**kwargs):
    """Charge les données clinical trials CSV dans XCom.
    
    """
    clinical_trials_df = load_csv_to_dataframe('./data/clinical_trials.csv')
    
    if 'date' in clinical_trials_df.columns:
        clinical_trials_df['date'] = clinical_trials_df['date'].apply(lambda x: pd.to_datetime(x).strftime('%d/%m/%Y'))
    
    return clinical_trials_df.to_json(orient='records')  # Convertir en JSON pour XCom

def load_pubmed(**kwargs):
    """Charge les données PubMed CSV dans XCom.
    
    """
    pubmed_df = load_csv_to_dataframe('./data/pubmed_.csv')
    
    # Convertir les colonnes de type datetime en chaînes
    if 'date' in pubmed_df.columns:
        pubmed_df['date'] = pd.to_datetime(pubmed_df['date']).dt.strftime('%Y-%m-%d')

    return pubmed_df.to_json(orient='records')  # Convertir en JSON pour XCom

def load_drugs(**kwargs):
    """Load drugs CSV into XCom.
    
    """
    drugs_df = load_csv_to_dataframe('./data/drugs.csv')
    
    return drugs_df.to_json(orient='records')  # Convertir en JSON pour XCom

def save_clinical_trials_raw(**kwargs):
    """Save raw clinical trials data.
    
    """
    ti = kwargs['ti']
    clinical_trials_json = ti.xcom_pull(task_ids='load_clinical_trials')
    clinical_trials_df = pd.read_json(clinical_trials_json)
    save_raw_data(clinical_trials_df, 'clinical_trials_raw.csv')

def save_pubmed_raw(**kwargs):
    """Save raw PubMed data.
    
    """
    ti = kwargs['ti']
    pubmed_json = ti.xcom_pull(task_ids='load_pubmed')
    pubmed_df = pd.read_json(pubmed_json)
    save_raw_data(pubmed_df, 'pubmed_raw.csv')

def save_drugs_raw(**kwargs):
    """Save raw drugs data.
    
    """
    ti = kwargs['ti']
    drugs_json = ti.xcom_pull(task_ids='load_drugs')
    drugs_df = pd.read_json(drugs_json)
    save_raw_data(drugs_df, 'drugs_raw.csv')

def reconcile_dataframes(**kwargs):
    """Reconcile DataFrames and produce JSON output.
    
    """
    ti = kwargs['ti']
    
    # Récupérer les DataFrames des XCom
    clinical_trials_json = ti.xcom_pull(task_ids='load_clinical_trials')
    pubmed_json = ti.xcom_pull(task_ids='load_pubmed')
    drugs_json = ti.xcom_pull(task_ids='load_drugs')

    # Convertir de JSON à DataFrame
    clinical_trials_df = pd.read_json(clinical_trials_json)
    pubmed_df = pd.read_json(pubmed_json)
    drugs_df = pd.read_json(drugs_json)

    # Création de la structure de graphe
    graph_data = []

    # Traitement des essais cliniques
    for index, row in clinical_trials_df.iterrows():
        trial_title = row['scientific_title']  
        trial_date = row['date']     
        journal_name = row.get('journal')  
        
        for drug in drugs_df['drug']:
            if drug.lower() in trial_title.lower():
                graph_data.append({
                    'drug': drug,
                    'publication': trial_title,
                    'date': str(trial_date),
                    'source': 'clinical_trial'
                })

    # Traitement des publications PubMed
    for index, row in pubmed_df.iterrows():
        pubmed_title = row['title']  
        pubmed_date = row['date']     
        journal_name = row.get('journal')  

        for drug in drugs_df['drug']:
            if drug.lower() in pubmed_title.lower():
                graph_data.append({
                    'drug': drug,
                    'publication': pubmed_title,
                    'date': str(pubmed_date),
                    'source': 'pubmed'
                })

    # Sauvegarder le graphe en JSON
    output_file = 'dags/output/graph.json'  
    with open(output_file, 'w') as json_file:
        json.dump(graph_data, json_file, indent=4)

    print(f"Graphe enregistré dans {output_file}")

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('data_pipeline_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    task_load_clinical_trials = PythonOperator(
        task_id='load_clinical_trials',
        python_callable=load_clinical_trials,
        provide_context=True,
    )

    task_save_clinical_trials_raw = PythonOperator(
        task_id='save_clinical_trials_raw',
        python_callable=save_clinical_trials_raw,
        provide_context=True,
    )

    task_load_pubmed = PythonOperator(
        task_id='load_pubmed',
        python_callable=load_pubmed,
        provide_context=True,
    )

    task_save_pubmed_raw = PythonOperator(
        task_id='save_pubmed_raw',
        python_callable=save_pubmed_raw,
        provide_context=True,
    )

    task_load_drugs = PythonOperator(
        task_id='load_drugs',
        python_callable=load_drugs,
        provide_context=True,
    )

    task_save_drugs_raw = PythonOperator(
        task_id='save_drugs_raw',
        python_callable=save_drugs_raw,
        provide_context=True,
    )

    task_reconcile = PythonOperator(
        task_id='reconcile_dataframes',
        python_callable=reconcile_dataframes,
        provide_context=True,
    )

    # Les dépendances
    task_load_clinical_trials >> task_save_clinical_trials_raw >> task_reconcile
    task_load_pubmed >> task_save_pubmed_raw >> task_reconcile
    task_load_drugs >> task_save_drugs_raw >> task_reconcile
