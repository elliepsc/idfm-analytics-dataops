# Dockerfile pour Airflow avec dépendances custom
# ==============================================

FROM apache/airflow:2.8.1-python3.11

USER root

# Installation des dépendances système si nécessaire
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copie des requirements depuis la racine du projet
COPY requirements.txt /tmp/requirements.txt

# Installation des dépendances Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Vérification des packages critiques
RUN python -c "import google.cloud.bigquery; import dbt.cli.main; import pandas; print('✅ All packages installed successfully')"

# Variables d'environnement par défaut
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/ingestion:/opt/airflow/scripts"
