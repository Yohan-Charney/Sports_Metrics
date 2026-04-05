# SportMetrics : Plateforme d'Analytics & Prédiction de Performance

## Contexte du Projet : Les "Foufous de Sochaux" (FFS)

Le club professionnel des **Foufous de Sochaux** traverse une phase de transformation majeure. Malgré l'acquisition massive de données via des capteurs biométriques (IoT), le staff technique et médical rencontrait des difficultés pour corréler la fatigue des joueurs avec les résultats sportifs et l'augmentation des blessures en milieu de saison.

**Objectifs de la mission :**
* **Load Management :** Prévenir les blessures en pilotant l'intensité des entraînements par la donnée.
* **ADN de la Victoire :** Identifier les indicateurs statistiques critiques qui différencient une victoire d'une défaite.
* **Optimisation du Roster :** Profiler les joueurs pour aider le coach dans ses rotations et le choix du "5 de départ".

---

## Architecture Technique (Pipeline ELT)

L'architecture a été conçue pour garantir une fraîcheur maximale des données. Le pipeline automatise la collecte et la transformation chaque matin pour que le staff dispose des indicateurs à jour avant l'entraînement quotidien.

### 1. Ingestion & Orchestration (N8N & Airflow)
* **Sources de données :** Les données biométriques (IoT), les journaux d'entraînement et les statistiques de match sont collectés via des formulaires et centralisés dans des fichiers **Google Sheets**.
* **Collecte (N8N) :** Nous utilisons **N8N** pour automatiser l'extraction des données depuis les API Google Sheets et assurer leur transfert vers notre Data Warehouse.
* **Orchestration (Apache Airflow) :** Le workflow complet est piloté par **Airflow**. Chaque soir à 00h00, Airflow déclenche séquentiellement l'ingestion, les transformations dbt.

  ![Architecture](Images/Architecture%20Sport%20Metrics.png)


### 2. Stockage & Transformation (BigQuery & dbt)

* **Data Warehouse :** Stockage centralisé sur **Google BigQuery**.
* **Transformation dbt : Architecture en Galaxie (Fact Constellation)**
    * **Staging :** Nettoyage des données brutes, typage strict (safe_cast), dédoublonnage (qualify row_number()), conversion des minutes MM:SS en décimales, parsing des dates multilingues.
    * **Intermediate :** Calcul de l'Index de Fatigue (FI) pondéré (charge interne 30%, charge externe 40%, récupération 30%). Filtre "Garbage Time" : exclusion des joueurs sous 5 minutes de jeu.
    * **Marts : Modèle en Galaxie**
    * Deux dimensions communes (Dim_Calendar_Match, Dim_Players_Info) alimentent trois tables de faits spécialisées : mart_game, mart_player, mart_physical_condition.
* **Data Quality :** Tests dbt automatisés (Unicité, Not Null) garantissant que les données du matin sont complètes avant d'atteindre le dashboard.

  ![Graph DBT](Images/Graph_dbt_Sport_Metrics.png)

  
---

## Intelligence Artificielle & Machine Learning
L'analyse passe du descriptif au prédictif via un **Notebook Jupyter** structuré autour de trois modèles clés :

### 1. Prédiction des Blessures (Modèle XGBoost)
* **Objectif :** Anticiper les arrêts médicaux avant qu'ils ne surviennent.
* **Méthodologie :** Utilisation d'un algorithme `XGBClassifier` avec ajustement du `scale_pos_weight` pour gérer le déséquilibre des classes (les blessures étant rares).  Feature engineerée : ratio ACWR (fatigue 7j / fatigue 28j).
* **Résultat stratégique :** Adoption d'un **seuil de probabilité à 0.2**. Ce réglage privilégie le *Recall* (taux de détection) pour garantir la sécurité des athlètes.
* **Directive Coach :** Dès que la probabilité dépasse **0.2**, une réduction automatique de **15% de l'intensité d'entraînement** est préconisée.

   ![Metriques XGBoost](Images/Recherche%20du%20seuil%20-%20JupyterLab%20-%20%5Blocalhost%5D.png)
   ![Metriques XGBoost](Images/Resultats%20Seuil%20et%20precision%20Blessure%20-%20JupyterLab%20-%20%5Blocalhost%5D.png)

### 2. Clustering des Profils (K-Means)
* **Objectif :** Pour la derniere saison,  Segmenter les joueurs par impact réel plutôt que par poste officiel.
* **Résultat :** K-Means avec k=3 déterminé par la méthode du coude, sur 8 variables de match normalisées (StandardScaler)
* **3 profils identifiés :**

Profil 0 Lieutenant All-Star : 14,49 pts / 4,38 passes. Créateurs de jeu à protéger.
Profil 1 Pivot Dominant : 21 pts / 11,68 rebonds / 3,47 contres. Un seul joueur dans ce cluster.
Profil 2 Spécialistes du Banc : 6,26 pts / +/- de -4,37. Recrues en phase d'intégration.

  ![Courbe Elbow](Images/Recherche_cluster%20-%20JupyterLab%20-%20%5Blocalhost%5D.png)


### 3. Focus Joueur : Lucas Dubois
* **Analyse :** Bien que classé statistiquement comme remplaçant ("Bench"), le modèle de clustering l'identifie comme un profil de "Star" (Cluster 0). Son impact collectif  en fait le **6ème homme stratégique** à intégrer dans le 5 de départ lors des matchs serrés.

  ![Kmean](Images/Top_joueurs_clusters%20-%20JupyterLab%20-%20%5Blocalhost%5D.png)


---

## Dashboard Power BI "Performance 360"
Une solution de Business Intelligence interactive offrant une vision à 360 au Staff Technique.

### Page 1 : Load Management & Santé
* **Alertes Risques :** Monitoring dynamique avec mise en forme conditionnelle (Vert/Orange/Rouge) pilotée par les prédictions du modèle ML.
* **Efficience de Récupération :** Suivi du ratio entre le repos réel et le besoin physiologique de chaque joueur.

  ![Power Bi](Images/Load%20management.jpg)

### Page 2 : Statistiques & Stratégie
* **Corrélation Performance/Fatigue :** Analyse visuelle de la chute de l'adresse lors des pics de fatigue.
* **Facteurs de Victoire :** Comparaison des métriques clés (Turnovers, Steals, Rebounds) entre les matchs gagnés et perdus.

  ![Power Bi](Images/Performance%20team.jpg)


### Page 3 : Optimisation du Lineup
* **Composition du 5 :** Suggestion de lineups basées sur la complémentarité des clusters de joueurs.

  ![Power Bi](Images/Lineup%20-%20Sport_Metrics%20-%20Power%20BI.png)


---

## ⚙️ Gestion de Projet Agile
Le projet a été piloté via une méthodologie **Agile/Kanban** sur **Trello** :
* **Workflow :** Backlog → A faire → En cours → A valider → Terminé.
* **Collaboration :** Simulation des interactions entre les besoins du staff médical, les contraintes du coach et les livrables Data.

---

## Conclusion
Grâce à ce pipeline complet, **SportMetrics** permet aux Foufous de Sochaux de transformer des données IoT complexes en **actions concrètes**. L'outil permet de protéger la santé des cadres tout en optimisant les performances collectives grâce à une gestion scientifique de la charge de travail.
