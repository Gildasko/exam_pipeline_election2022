# Exam Pipeline election2022

# DATA COLLECT ( NIFI )
Objectif : Collecter les informations liées aux élection présidentielle des 10 et 24 avril 2022 - Résultats définitifs , les analyser et faire 
faire une transformations des données ( tour1 et tout2 ) afin de savoir les régions propices pour chaque candidat. Utiliser l'ensemble des technologies
appris en cours et préciser les outils externes pris en compte.

Démarches : 
On s'est référé au site data.gouv ( https://www.data.gouv.fr/fr/datasets/election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-1er-tour/ : pour le premier tour)
afin d'avoir des jeux de données exactes et plus fiables. 

#Collecte des données avec NIFI 

![Nifi Flow](https://raw.githubusercontent.com/Gildasko/exam_pipeline_election2022/img/un.png)

On a conçu un pipeline de données avec Nifi en partant d'un processor ( invoqueHTTP) qui sera chargé de récupéré le flux de données directement sur le site 
data.gouv. 

#Renommer le flux pour la sauvegarde en disque 
Afin de mieux traiter le flux de données , on a utilisé un processeur updateAttribute pour le renommer et le convertir directement en csv dans les propriétés.

#Ecriture en disque 
On a mis en place un processeur PutFile afin de sauvegarder notre jeu de données (ici 1tour2022.csv pour le 1er tour et 1tour2022.csv pour le second tour) et
de pouvoir le traiter dans spark. 

#Topic Kafka

Le processeur Kafka reçoit aussi le flux de données pour l'afficher sur sa console. 

# DATA TRANSFORM (SPARK)


# GESTION DES TASKS (AIRFLOW)

# INTEGRATION AIRFLOW - NIFI - SPARK


