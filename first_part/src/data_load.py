import pandas as pd
import json
import os

class DataLoader:
    def __init__(self, config):
        """
        Initialise le DataLoader avec les chemins des fichiers à partir d'un fichier de configuration.

        :param config: Dictionnaire ou fichier de configuration contenant les chemins des fichiers.
        """
        self.drugs_path = config.get("drugs_path")
        self.pubmed_csv_path = config.get("pubmed_csv_path")
        self.pubmed_json_path = config.get("pubmed_json_path")
        self.clinical_trials_path = config.get("clinical_trials_path")

    def load_csv(self, filepath):
        """
        Charge un fichier CSV en DataFrame Pandas.
        
        :param filepath: Chemin du fichier CSV.
        :return: DataFrame Pandas contenant les données.
        """
        df = pd.read_csv(filepath)
        print(f"{os.path.basename(filepath)} chargé avec succès.")
        return df

    def load_json(self, filepath):
        """
        Charge un fichier JSON en DataFrame Pandas.
        
        :param filepath: Chemin du fichier JSON.
        :return: DataFrame Pandas contenant les données.
        """
        with open(filepath, 'r') as file:
            data = json.load(file)
        df = pd.DataFrame(data)
        print(f"{os.path.basename(filepath)} chargé avec succès.")
        return df

    def clean_json(self, filepath):
        """
        Nettoie le fichier JSON en supprimant les virgules en trop.
        
        :param filepath: Chemin du fichier JSON à corriger.
        """
        # Lire le contenu du fichier
        with open(filepath, 'r') as file:
            content = file.read()

        # Suppression des caractères indésirables
        cleaned_content = content.strip()

        # Enlever les virgules en trop à la fin de chaque objet ou tableau
        cleaned_content = cleaned_content.replace(',\n}', '}')  # Pour les objets
        cleaned_content = cleaned_content.replace(',\n]', ']')  # Pour les tableaux

        # Charger le JSON après nettoyage
        data = json.loads(cleaned_content)

        # Écrire le JSON corrigé dans le même fichier
        with open(filepath, 'w') as file:
            json.dump(data, file, indent=4)

        print(f"{filepath} a été nettoyé avec succès.")

    def clean_and_load_json(self, filepath):
        """
        Nettoie le fichier JSON et tente de le charger en DataFrame.
        
        :param filepath: Chemin du fichier JSON à nettoyer.
        :return: DataFrame Pandas contenant les données ou None en cas d'échec.
        """
        self.clean_json(filepath)  # Nettoyer le fichier JSON en premier
        
        # Charger le fichier JSON après nettoyage
        return self.load_json(filepath)

    def load_all_data(self):
        """
        Charge tous les fichiers de données et les retourne sous forme de DataFrames.
        
        :return: Tuple contenant les DataFrames pour chaque fichier.
        """
        drugs_df = self.load_csv(self.drugs_path)
        pubmed_csv_df = self.load_csv(self.pubmed_csv_path)

        # Nettoyer et charger le fichier JSON
        pubmed_json_df = self.clean_and_load_json(self.pubmed_json_path)

        clinical_trials_df = self.load_csv(self.clinical_trials_path)

        return drugs_df, pubmed_csv_df, pubmed_json_df, clinical_trials_df
