# =======================================================================================
#           Prevision Equipe - KMeans Clustering - SportMetrics
#           Executé automatiquement par Airflow après dbt
# =======================================================================================

import os
import logging
from google.cloud import bigquery
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================================
# 1. Connexion BigQuery
# ==================================
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ["GCP_DATASET_ID"]

client = bigquery.Client(project=PROJECT_ID)

# ==================================
# 2. Chargement des données
# ==================================
logger.info("Chargement des données depuis BigQuery...")

query = f"""
    SELECT
        player_id,
        player_name,
        Position,
        Start_position,
        AVG(Points) as Points,
        AVG(Assists) as Assists,
        AVG(Total_rebounds) as Total_rebounds,
        AVG(Steals) as Steals,
        AVG(Blocks) as Blocks,
        AVG(Turnover) as Turnover,
        AVG(Player_fault) as Player_fault,
        AVG(Plus_minus) as Plus_minus,
        round(AVG(Performance_score_match),2) as Performance_score_match,
        round(AVG(Performance_score_match_min),2) as Performance_score_match_min,
        round(AVG(minutes_played),2) as minutes_played
    FROM `{PROJECT_ID}.{DATASET_ID}.mart_player`
    WHERE minutes_played >= 5 AND Season = '2023-2024'
    GROUP BY Season, player_id, player_name, Position, Start_position
"""

df = client.query(query).to_dataframe()
logger.info(f"Données chargées : {len(df)} joueurs")

# ==================================
# 3. Clustering KMeans
# ==================================
features = [
    'Points', 'Assists', 'Total_rebounds', 'Steals',
    'Blocks', 'Turnover', 'Player_fault', 'Plus_minus'
]

X = df[features].fillna(0)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
df['Profil'] = kmeans.fit_predict(X_scaled)

logger.info("Clusters calculés :")
logger.info(df.groupby('Profil')[features].mean())

# ==================================
# 4. Écriture des résultats dans BigQuery
# ==================================
logger.info("Écriture des résultats dans BigQuery...")

results = df[[
    'player_id', 'player_name', 'Position', 'Start_position',
    'Profil', 'Performance_score_match', 'Performance_score_match_min',
    'minutes_played'
]].copy()

results['player_id'] = results['player_id'].astype(str)
results['Profil'] = results['Profil'].astype(int)

table_id = f"{PROJECT_ID}.{DATASET_ID}.ml_player_clusters"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    autodetect=True
)

job = client.load_table_from_dataframe(results, table_id, job_config=job_config)
job.result()

logger.info(f"Résultats écrits dans {table_id} : {len(results)} joueurs")
logger.info("Clustering equipe terminé avec succès")