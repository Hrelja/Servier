# Data Pipeline for Drug Mention Analysis

## Description

Ce projet met en place un pipeline de données qui extrait, nettoie et analyse des informations sur les médicaments, leurs mentions dans des publications scientifiques et des essais cliniques. Le pipeline produit un fichier JSON représentant un graphe de liaison entre les différents médicaments et leurs mentions respectives dans diverses publications, ainsi que les journaux associés et les dates correspondantes.

## Fonctionnalités

- Chargement de fichiers CSV pour les médicaments, les essais cliniques et les publications PubMed.
- Nettoyage et prétraitement des données, y compris la gestion des formats de date incohérents.
- Réconciliation des données entre les fichiers CSV et les mentions des médicaments.
- Génération d'un fichier JSON qui représente les relations entre les médicaments et les publications.

## Règles de gestion

- Un médicament est considéré comme mentionné dans un article PubMed ou un essai clinique s'il est mentionné dans le titre de la publication.
- Un médicament est considéré comme mentionné par un journal s'il est mentionné dans une publication émise par ce journal.

## Prérequis

Assurez-vous d'avoir installé les bibliothèques Python suivantes :

```bash
python -m venv .
pip install -r /path/to/requirements.txt
```

## Docker Compose pour lancer airflow en local
```bash
docker compose up
```

Sur un autre terminal lancer docker ps pour vérifier si les conteneurs sont bien lancés
```bash
docker ps
```

Aller sur le localhost:8080


