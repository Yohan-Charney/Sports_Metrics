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
* **Orchestration (Apache Airflow) :** Le workflow complet est piloté par **Airflow**. Chaque matin à 6h00, Airflow déclenche séquentiellement l'ingestion, les transformations dbt.

  ![Architecture](Images//Architecture%20Sport%20Metrics.png)


### 2. Stockage & Transformation (BigQuery & dbt)

* **Data Warehouse :** Stockage centralisé sur **Google BigQuery**.
* **Transformation dbt :**
    * **Staging :** Nettoyage des données Sheets, typage strict et dédoublonnage.
    * **Intermediate :** Calcul complexe de l'**Index de Fatigue (FI)** pondéré.
    * **Marts :** Modélisation en schéma en étoile optimisée pour les performances Power BI.
* **Data Quality :** Tests dbt automatisés (Unicité, Not Null, Accepted Values) garantissant que les données du matin sont complètes avant d'atteindre le dashboard.
  
---

## Intelligence Artificielle & Machine Learning
L'analyse passe du descriptif au prédictif via un **Notebook Jupyter** structuré autour de trois modèles clés :

### 1. Prédiction des Blessures (Modèle XGBoost)
* **Objectif :** Anticiper les arrêts médicaux avant qu'ils ne surviennent.
* **Méthodologie :** Utilisation d'un algorithme `XGBClassifier` avec ajustement du `scale_pos_weight` pour gérer le déséquilibre des classes (les blessures étant rares).
* **Résultat stratégique :** Adoption d'un **seuil de probabilité à 0.2**. Ce réglage privilégie le *Recall* (taux de détection) pour garantir la sécurité des athlètes.
* **Directive Coach :** Dès que la probabilité dépasse **0.2**, une réduction automatique de **15% de l'intensité d'entraînement** est préconisée.

   ![Architecture](Images/Recherche%20du%20seuil%20-%20JupyterLab%20-%20%5Blocalhost%5D.png)
   ![Architecture](Images/Resultats%20Seuil%20et%20precision%20Blessure%20-%20JupyterLab%20-%20%5Blocalhost%5D.png)

### 2. Clustering des Profils (K-Means)
* **Objectif :** Pour la derniere saison, regrouper les joueurs selon leur impact réel sur le terrain plutôt que par leur poste officiel.
* **Résultat :** Identification de 5 clusters distincts, des protecteurs du cerle à l'organisateur du jeu offensif

### 3. Focus Joueur : David Roussel
* **Analyse :** Bien que classé statistiquement comme remplaçant ("Bench"), le modèle de clustering l'identifie comme un profil de "Star" (Cluster 3). Son impact collectif positif (`Plus_minus`) en fait le **6ème homme stratégique** à intégrer dans le 5 de départ lors des matchs serrés.

---

## Dashboard Power BI "Performance 360"
Une solution de Business Intelligence interactive offrant une vision à 360 au Staff Technique.

### Page 1 : Load Management & Santé
* **Alertes Risques :** Monitoring dynamique avec mise en forme conditionnelle (Vert/Orange/Rouge) pilotée par les prédictions du modèle ML.
* **Efficience de Récupération :** Suivi du ratio entre le repos réel et le besoin physiologique de chaque joueur.

### Page 2 : Statistiques & Stratégie
* **Corrélation Performance/Fatigue :** Analyse visuelle de la chute de l'adresse lors des pics de fatigue.
* **Facteurs de Victoire :** Comparaison des métriques clés (Turnovers, Steals, Rebounds) entre les matchs gagnés et perdus.

### Page 3 : Optimisation du Lineup
* **Composition du 5 :** Suggestion de lineups basées sur la complémentarité des clusters de joueurs.

---

## ⚙️ Gestion de Projet Agile
Le projet a été piloté via une méthodologie **Agile/Kanban** sur **Trello** :
* **Workflow :** Backlog → To Do → In Progress → QA Testing (dbt tests) → Done.
* **Collaboration :** Simulation des interactions entre les besoins du staff médical, les contraintes du coach et les livrables Data.

---

## Conclusion
Grâce à ce pipeline complet, **SportMetrics** permet aux Foufous de Sochaux de transformer des données IoT complexes en **actions concrètes**. L'outil permet de protéger la santé des cadres tout en optimisant les performances collectives grâce à une gestion scientifique de la charge de travail.
