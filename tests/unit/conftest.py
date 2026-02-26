"""
pytest configuration and shared fixtures
"""

import pytest
from pathlib import Path
from unittest.mock import Mock
import json


@pytest.fixture
def mock_ods_response():
    """Mock d'une réponse API Opendatasoft V2"""
    return {
        'total_count': 2,
        'results': [
            {
                'record': {
                    'id': 'rec1',
                    'fields': {
                        'date': '2024-01-01',
                        'line': 'RER A',
                        'value': 100
                    }
                }
            },
            {
                'record': {
                    'id': 'rec2',
                    'fields': {
                        'date': '2024-01-02',
                        'line': 'RER B',
                        'value': 200
                    }
                }
            }
        ]
    }


@pytest.fixture
def sample_records():
    """Sample records extraits de l'API"""
    return [
        {
            'date': '2024-01-01',
            'line': 'RER A',
            'value': 100,
            'ingestion_ts': '2024-01-01T00:00:00',
            'source': 'test'
        },
        {
            'date': '2024-01-02',
            'line': 'RER B',
            'value': 200,
            'ingestion_ts': '2024-01-02T00:00:00',
            'source': 'test'
        }
    ]


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Temporary output directory for tests"""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_config():
    """Mock configuration YAML"""
    return {
        'idfm': {
            'base_url': 'https://data.iledefrance-mobilites.fr/api/explore/v2.1',
            'datasets': {
                'validations': {
                    'id': 'validations-sur-le-reseau-ferre',
                    'filters': {
                        'date_field': 'jour'
                    },
                    'fields': {
                        'date': 'jour',
                        'station': 'libelle_arret',
                        'validations': 'nb_vald'
                    }
                }
            }
        },
        'transilien': {
            'base_url': 'https://ressources.data.sncf.com/api/explore/v2.1',
            'datasets': {
                'punctuality': {
                    'id': 'regularite-mensuelle-transilien',
                    'filters': {
                        'date_field': 'date'
                    },
                    'fields': {
                        'date': 'date',
                        'line': 'ligne',
                        'punctuality_rate': 'taux_regularite'
                    }
                }
            }
        }
    }
