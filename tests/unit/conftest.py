"""
pytest configuration and shared fixtures

Mock structure aligned with real ODS API v2.1 (confirmed 2026-02-27):
  - Fields are at ROOT level of each record (not nested in record['record']['fields'])
  - Real example: {"jour": "2025-03-12", "nb_vald": 12, "libelle_arret": "..."}
"""

import pytest
from pathlib import Path
from unittest.mock import Mock
import json


@pytest.fixture
def mock_ods_response():
    """
    Mock of an Opendatasoft V2.1 API response.

    FIXED V2: fields at record root level, not in record['record']['fields'].
    Structure confirmed by curl 2026-02-27.
    """
    return {
        'total_count': 2,
        'results': [
            {
                'jour': '2024-01-01',
                'code_stif_arret': '401',
                'libelle_arret': 'CHATELET',
                'code_stif_trns': '810',
                'code_stif_res': '802',
                'categorie_titre': 'Navigo',
                'nb_vald': 1000
            },
            {
                'jour': '2024-01-02',
                'code_stif_arret': '402',
                'libelle_arret': 'GARE DU NORD',
                'code_stif_trns': '810',
                'code_stif_res': '802',
                'categorie_titre': 'Navigo',
                'nb_vald': 1500
            }
        ]
    }


@pytest.fixture
def mock_ods_response_punctuality():
    """Mock of a Transilien punctuality API response."""
    return {
        'total_count': 2,
        'results': [
            {
                'date': '2024-01',
                'service': 'RER',
                'ligne': 'A',
                'nom_de_la_ligne': 'RER A',
                'taux_de_ponctualite': 95.5
            },
            {
                'date': '2024-01',
                'service': 'RER',
                'ligne': 'B',
                'nom_de_la_ligne': 'RER B',
                'taux_de_ponctualite': 92.3
            }
        ]
    }


@pytest.fixture
def sample_records_validations():
    """Validation records after field mapping (transformed output)."""
    return [
        {
            'date': '2024-01-01',
            'stop_id': '401',
            'stop_name': 'CHATELET',
            'line_code_trns': '810',
            'line_code_res': '802',
            'ticket_type': 'Navigo',
            'validation_count': 1000,
            'ingestion_ts': '2024-01-01T00:00:00+00:00',
            'source': 'idfm_validations_rail'
        },
        {
            'date': '2024-01-02',
            'stop_id': '402',
            'stop_name': 'GARE DU NORD',
            'line_code_trns': '810',
            'line_code_res': '802',
            'ticket_type': 'Navigo',
            'validation_count': 1500,
            'ingestion_ts': '2024-01-02T00:00:00+00:00',
            'source': 'idfm_validations_rail'
        }
    ]


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Temporary output directory for tests."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_config():
    """
    Mock YAML configuration aligned with corrected apis.yml (V2).

    FIXED V2:
    - Key is 'validations_rail' (not 'validations')
    - Real API field names (jour, nb_vald, taux_de_ponctualite, etc.)
    - Corrected punctuality dataset fields
    """
    return {
        'idfm': {
            'base_url': 'https://data.iledefrance-mobilites.fr/api/explore/v2.1',
            'datasets': {
                'validations_rail': {
                    'id': 'validations-reseau-ferre-nombre-validations-par-jour-1er-trimestre',
                    'filters': {
                        'date_field': 'jour'
                    },
                    'fields': {
                        'date': 'jour',
                        'stop_id': 'code_stif_arret',
                        'stop_name': 'libelle_arret',
                        'line_code_trns': 'code_stif_trns',
                        'line_code_res': 'code_stif_res',
                        'ticket_type': 'categorie_titre',
                        'validation_count': 'nb_vald'
                    }
                }
            }
        },
        'transilien': {
            'base_url': 'https://ressources.data.sncf.com/api/explore/v2.1',
            'datasets': {
                'punctuality': {
                    'id': 'ponctualite-mensuelle-transilien',
                    'filters': {
                        'date_field': 'date'
                    },
                    'fields': {
                        'month': 'date',
                        'service': 'service',
                        'line_id': 'ligne',
                        'line_name': 'nom_de_la_ligne',
                        'punctuality_rate': 'taux_de_ponctualite'
                    }
                }
            }
        }
    }
