"""
Unit tests for odsv2_client.py

Behaviors tested:
  - Initialization: base_url stored, dataset_url correctly constructed
  - get_records: correct HTTP call, response parsed and returned
  - extract_fields: reads from nested record['record']['fields'] (legacy structure kept for compatibility)
  - _get_nested_field: dot notation support for nested dicts
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Direct import (not via 'ingestion.' package prefix)
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'ingestion'))

from odsv2_client import ODSv2Client


class TestODSv2ClientInit:

    def test_init_base_url_strips_trailing_slash(self):
        """base_url stored without trailing slash."""
        client = ODSv2Client(
            base_url="https://example.com/api/v2.1/",
            dataset_id="test-dataset"
        )
        assert client.base_url == "https://example.com/api/v2.1"

    def test_init_dataset_id_stored(self):
        """dataset_id correctly stored."""
        client = ODSv2Client(
            base_url="https://example.com/api/v2.1",
            dataset_id="my-dataset"
        )
        assert client.dataset_id == "my-dataset"

    def test_init_dataset_url_constructed(self):
        """dataset_url built from base_url + dataset_id + /records."""
        client = ODSv2Client(
            base_url="https://example.com/api/v2.1",
            dataset_id="test-dataset"
        )
        assert "test-dataset" in client.dataset_url
        assert "records" in client.dataset_url


class TestGetRecords:

    @patch('odsv2_client.requests.Session')
    def test_get_records_calls_correct_url(self, mock_session_class):
        """get_records calls the correct dataset endpoint."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {'total_count': 0, 'results': []}
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = ODSv2Client("https://example.com/api/v2.1", "test-dataset")
        client.session = mock_session

        client.get_records(limit=10)

        mock_session.get.assert_called_once()
        call_url = mock_session.get.call_args[0][0]
        assert "test-dataset" in call_url

    @patch('odsv2_client.requests.Session')
    def test_get_records_returns_results(self, mock_session_class):
        """get_records returns the full API response dict."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'total_count': 2,
            'results': [
                {'jour': '2024-01-01', 'nb_vald': 100},
                {'jour': '2024-01-02', 'nb_vald': 200},
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = ODSv2Client("https://example.com/api/v2.1", "test")
        client.session = mock_session

        result = client.get_records()

        assert result['total_count'] == 2
        assert len(result['results']) == 2


class TestExtractFields:
    """
    extract_fields reads from record['record']['fields'] (legacy ODS structure).

    Note: production scripts (extract_validations.py, extract_ponctuality.py)
    no longer use this method — they read fields from record root directly.
    This method is kept for backward compatibility and tested here accordingly.
    """

    def test_extract_fields_from_nested_structure(self):
        """extract_fields reads from record['record']['fields'] and renames keys."""
        client = ODSv2Client("https://example.com/api/v2.1", "test")

        records = [
            {'record': {'fields': {'source_id': '123', 'source_name': 'Test', 'extra': 'ignore'}}},
            {'record': {'fields': {'source_id': '456', 'source_name': 'Test2', 'extra': 'ignore'}}},
        ]

        field_mapping = {'id': 'source_id', 'name': 'source_name'}
        extracted = client.extract_fields(records, field_mapping)

        assert len(extracted) == 2
        assert extracted[0] == {'id': '123', 'name': 'Test'}
        assert 'extra' not in extracted[0]

    def test_extract_fields_missing_field_returns_none(self):
        """Missing field in source record returns None, not an exception."""
        client = ODSv2Client("https://example.com/api/v2.1", "test")

        records = [{'record': {'fields': {'only_field': 'value'}}}]
        field_mapping = {'existing': 'only_field', 'missing': 'nonexistent_field'}

        extracted = client.extract_fields(records, field_mapping)

        assert extracted[0]['existing'] == 'value'
        assert extracted[0]['missing'] is None


class TestGetNestedField:

    def test_simple_field(self):
        """Simple field without dot notation."""
        client = ODSv2Client("https://example.com/api/v2.1", "test")
        data = {'name': 'Paris'}
        assert client._get_nested_field(data, 'name') == 'Paris'

    def test_nested_field_dot_notation(self):
        """Nested field accessed via dot notation."""
        client = ODSv2Client("https://example.com/api/v2.1", "test")
        data = {'address': {'city': 'Paris'}}
        assert client._get_nested_field(data, 'address.city') == 'Paris'

    def test_missing_field_returns_none(self):
        """Missing field returns None, not an exception."""
        client = ODSv2Client("https://example.com/api/v2.1", "test")
        data = {'name': 'Paris'}
        assert client._get_nested_field(data, 'nonexistent') is None
