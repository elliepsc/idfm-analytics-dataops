"""
Unit tests for extract_validations.py

Behaviors tested:
  - Successful extraction → JSON file created with correct content
  - No records returned → no file created, no exception raised
  - Non-existent output directory → created automatically
  - Metadata fields added (ingestion_ts, source)
  - Output filename matches expected pattern
  - Correct config key used ('validations_rail', not 'validations')
  - ingestion_ts is timezone-aware (not deprecated utcnow())
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'ingestion'))

from extract_validations import extract_validations, load_config


class TestExtractValidations:

    @patch('extract_validations.load_config')
    @patch('extract_validations.ODSv2Client')
    def test_extract_validations_success(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        mock_ods_response,
        tmp_output_dir
    ):
        """Successful extraction: JSON file created with correct content and field mapping."""
        mock_load_config.return_value = mock_config

        mock_client = MagicMock()
        # FIXED V2: get_all_records returns root-level records directly
        mock_client.get_all_records.return_value = mock_ods_response['results']
        mock_client_class.return_value = mock_client

        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-02',
            output_dir=str(tmp_output_dir)
        )

        # get_all_records called, extract_fields NOT called (we read root directly)
        mock_client.get_all_records.assert_called_once()
        mock_client.extract_fields.assert_not_called()

        # Output file created with correct name
        output_files = list(tmp_output_dir.glob('*.json'))
        assert len(output_files) == 1
        assert output_files[0].name == 'validations_2024-01-01_2024-01-02.json'

        # Content is correct
        with open(output_files[0]) as f:
            data = json.load(f)

        assert len(data) == 2
        # Metadata fields present
        assert 'ingestion_ts' in data[0]
        assert data[0]['source'] == 'idfm_validations_rail'
        # Fields correctly mapped from record root
        assert data[0]['date'] == '2024-01-01'
        assert data[0]['stop_id'] == '401'
        assert data[0]['stop_name'] == 'CHATELET'
        assert data[0]['validation_count'] == 1000
        assert data[0]['ticket_type'] == 'Navigo'

    @patch('extract_validations.load_config')
    @patch('extract_validations.ODSv2Client')
    def test_extract_validations_no_records(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        tmp_output_dir
    ):
        """No records returned → no file created, no exception raised."""
        mock_load_config.return_value = mock_config

        mock_client = MagicMock()
        mock_client.get_all_records.return_value = []
        mock_client_class.return_value = mock_client

        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-02',
            output_dir=str(tmp_output_dir)
        )

        output_files = list(tmp_output_dir.glob('*.json'))
        assert len(output_files) == 0

    @patch('extract_validations.load_config')
    @patch('extract_validations.ODSv2Client')
    def test_extract_validations_creates_directory(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        mock_ods_response,
        tmp_path
    ):
        """Non-existent output directory is created automatically when records are found.

        Note: directory is only created if records exist — no empty dirs on empty results.
        """
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        # Need actual records — mkdir only runs after the empty-records early return
        mock_client.get_all_records.return_value = mock_ods_response['results']
        mock_client_class.return_value = mock_client

        output_dir = tmp_path / "new" / "nested" / "dir"
        assert not output_dir.exists()

        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-02',
            output_dir=str(output_dir)
        )

        assert output_dir.exists()
        assert output_dir.is_dir()

    @patch('extract_validations.load_config')
    @patch('extract_validations.ODSv2Client')
    def test_uses_correct_config_key(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        mock_ods_response,
        tmp_output_dir
    ):
        """ODSv2Client is instantiated with the correct dataset ID from 'validations_rail' key."""
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = mock_ods_response['results']
        mock_client_class.return_value = mock_client

        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-01',
            output_dir=str(tmp_output_dir)
        )

        call_kwargs = mock_client_class.call_args
        assert 'validations-reseau-ferre' in str(call_kwargs)

    @patch('extract_validations.load_config')
    @patch('extract_validations.ODSv2Client')
    def test_ingestion_ts_is_timezone_aware(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        mock_ods_response,
        tmp_output_dir
    ):
        """ingestion_ts must be timezone-aware (datetime.now(timezone.utc), not deprecated utcnow())."""
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = mock_ods_response['results']
        mock_client_class.return_value = mock_client

        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-01',
            output_dir=str(tmp_output_dir)
        )

        output_files = list(tmp_output_dir.glob('*.json'))
        with open(output_files[0]) as f:
            data = json.load(f)

        # Timezone-aware timestamps contain '+00:00' or 'Z'
        assert '+00:00' in data[0]['ingestion_ts'] or 'Z' in data[0]['ingestion_ts']

    def test_load_config_is_callable(self):
        """load_config is a callable function."""
        assert callable(load_config)
