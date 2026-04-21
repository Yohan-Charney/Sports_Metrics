# =======================================================================================
#           Prevention Blessure - XGBoost - SportMetrics
#           Executé automatiquement par Airflow après dbt
# =======================================================================================

import os
import logging
from google.cloud import bigquery
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

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
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.mart_physical_condition`
"""

df = client.query(query).to_dataframe()
logger.info(f"Données chargées : {len(df)} lignes")

# ==================================
# 3. Préparation des features
# ==================================
features = [
    'Fi_before_match', 'fi_avg_7d', 'fi_max_7d', 'training_load_7d',
    'fi_avg_28d', 'training_load_28d', 'Focus_Level', 'minutes_played', 'Position'
]
df = df[df["Season"] == '2023-2024']
df = df.dropna(subset=features + ['Injury_Risk'])

df['Position'] = LabelEncoder().fit_transform(df['Position'])
df['ACWR'] = df['fi_avg_7d'] / (df['fi_avg_28d'] + 1e-9)

X = df[features + ['ACWR']]
y = df['Injury_Risk']

# ==================================
# 4. Entraînement XGBoost
# ==================================
logger.info("Entraînement du modèle XGBoost...")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

neg = (y_train == 0).sum()
pos = (y_train == 1).sum()
scale_pos_weight = neg / pos

xgb_model = XGBClassifier(
    n_estimators=200,
    max_depth=5,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    scale_pos_weight=scale_pos_weight,
    eval_metric="logloss"
)

xgb_model.fit(X_train, y_train)

# ==================================
# 5. Prédictions sur toutes les données
# ==================================
logger.info("Calcul des prédictions...")

df['injury_probability'] = xgb_model.predict_proba(X[features + ['ACWR']])[:, 1]
df['injury_alert'] = (df['injury_probability'] >= 0.20).astype(int)
def get_reduction(prob):
    if prob >= 0.70:
        return 80
    elif prob >= 0.50:
        return 50
    elif prob >= 0.20:
        return 15
    else:
        return 0

df['training_intensity_reduction'] = df['injury_probability'].apply(get_reduction)

# ==================================
# 6. Écriture des résultats dans BigQuery
# ==================================
logger.info("Écriture des résultats dans BigQuery...")

results = df[[
    'player_id', 'session_id', 'injury_probability',
    'injury_alert', 'training_intensity_reduction'
]].copy()

results['player_id'] = results['player_id'].astype(str)

table_id = f"{PROJECT_ID}.{DATASET_ID}.ml_injury_predictions"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    autodetect=True
)

job = client.load_table_from_dataframe(results, table_id, job_config=job_config)
job.result()

logger.info(f"Résultats écrits dans {table_id} : {len(results)} lignes")
logger.info("Prevention blessure terminée avec succès")