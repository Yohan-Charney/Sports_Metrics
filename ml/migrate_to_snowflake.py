import os
import logging
from google.cloud import bigquery
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "n8n-automation-485809"
DATASET_ID = "Sport_Metrics"

TABLES = [
    "team_players_personal_info",
    "team_players_stats",
    "team_boxscores",
    "team_games_dataset",
    "team_training_sessions"
]

logger.info("Connexion BigQuery...")
bq_client = bigquery.Client(project=PROJECT_ID)

logger.info("Connexion Snowflake...")
sf_conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse="SPORT_METRICS_WH",
    database="SPORT_METRICS",
    schema="RAW"
)

for table in TABLES:
    logger.info(f"Migration de {table}...")
    df = bq_client.query(
        f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table}`"
    ).to_dataframe()
    logger.info(f"{len(df)} lignes lues depuis BigQuery")
    df.columns = df.columns.str.upper() 
    success, nchunks, nrows, _ = write_pandas(
        sf_conn, df, table.upper(),
        auto_create_table=True,
        overwrite=True
    )
    logger.info(f"{nrows} lignes ecrites dans Snowflake.RAW.{table} ok")

sf_conn.close()
logger.info("Migration terminee")