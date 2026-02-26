# Tests unitaires du client ODS
import pytest
from unittest.mock import Mock, patch
from ingestion.odsv2_client import ODSv2Client


class TestODSv2Client:
    """Tests du client Opendatasoft V2"""

    def test_init(self):
        """Test initialisation"""
        client = ODSv2Client(
            base_url="https://example.com/api/v2.1",
            dataset_id="test-dataset"
        )

        assert client.base_url == "https://example.com/api/v2.1"
        assert client.dataset_id == "test-dataset"
        assert "test-dataset" in client.endpoint

    @patch('requests.Session.get')
    def test_get_records_success(self, mock_get):
        """Test récupération réussie"""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            'total_count': 2,
            'results': [
                {'field1': 'value1', 'field2': 'value2'},
                {'field1': 'value3', 'field2': 'value4'}
            ]
        }
        mock_get.return_value = mock_response

        client = ODSv2Client("https://example.com/api/v2.1", "test")
        records = client.get_records(limit=10)

        assert len(records) == 2
        assert records[0]['field1'] == 'value1'

    def test_extract_fields(self):
        """Test extraction et renommage de champs"""
        client = ODSv2Client("https://example.com/api/v2.1", "test")

        records = [
            {'source_id': '123', 'source_name': 'Test', 'extra': 'ignore'},
            {'source_id': '456', 'source_name': 'Test2', 'extra': 'ignore'}
        ]

        field_mapping = {
            'id': 'source_id',
            'name': 'source_name'
        }

        extracted = client.extract_fields(records, field_mapping)

        assert len(extracted) == 2
        assert extracted[0] == {'id': '123', 'name': 'Test'}
        assert 'extra' not in extracted[0]
