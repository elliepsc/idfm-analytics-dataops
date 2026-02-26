# Vérification des SLA via fct_data_health_daily
import os
import sys
from google.cloud import bigquery
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


def check_sla():
    """
    Vérifie les SLA via la table fct_data_health_daily
    Retourne exit code 1 si breach détecté, 0 sinon
    """
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset = os.getenv('BQ_DATASET_ANALYTICS', 'transport_analytics')
    table = f"{project_id}.{dataset}.fct_data_health_daily"

    client = bigquery.Client(project=project_id)

    # Query pour détecter les breaches des 2 derniers jours
    query = f"""
    SELECT
      table_name,
      metric_date,
      freshness_hours,
      sla_hours,
      row_count,
      null_percentage,
      duplicate_count,
      sla_met
    FROM `{table}`
    WHERE metric_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
      AND sla_met = FALSE
    ORDER BY metric_date DESC, table_name
    """

    logger.info("Checking SLA compliance...")
    logger.info(f"Query: {query}")

    try:
        results = list(client.query(query).result())

        if len(results) == 0:
            logger.info("✅ SLA OK - No breaches detected")
            return 0

        # Breaches détectées
        logger.error(f"❌ SLA BREACH - {len(results)} violation(s) detected")

        for row in results:
            logger.error("")
            logger.error(f"Table: {row.table_name}")
            logger.error(f"Date: {row.metric_date}")
            logger.error(f"Freshness: {row.freshness_hours}h (SLA: {row.sla_hours}h)")
            logger.error(f"Row count: {row.row_count}")
            logger.error(f"Null %: {row.null_percentage:.2f}%")
            logger.error(f"Duplicates: {row.duplicate_count}")

        return 1

    except Exception as e:
        logger.error(f"Error checking SLA: {e}")
        return 1


if __name__ == '__main__':
    exit_code = check_sla()
    sys.exit(exit_code)
