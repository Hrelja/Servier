import yaml
import pandas as pd
from data_load import DataLoader

# Charger la configuration
with open("../config/config.yaml", "r") as file:
   config = yaml.safe_load(file)

# Initialiser le DataLoader
data_loader = DataLoader(config)

# Charger toutes les données
drugs_df, pubmed_csv_df, pubmed_json_df, clinical_trials_df = data_loader.load_all_data()

# Réconciliation des données pubmed_df
pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df]).drop_duplicates().reset_index(drop=True)
pubmed_df['date'] = pubmed_df['date'].apply(lambda x: pd.to_datetime(x).strftime('%d/%m/%Y'))
pubmed_df.to_csv('../data/pubmed_.csv', index=False)