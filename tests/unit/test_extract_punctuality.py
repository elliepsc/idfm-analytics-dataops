"""
Unit tests for extract_ponctuality.py

Behaviors tested:
  - Successful extraction → JSON file created with correct content
  - No records returned → no file created, no exception raised
  - Date truncation: YYYY-MM-DD input accepted and truncated to YYYY-MM for API
  - Metadata fields added (ingestion_ts, source)
  - Fields correctly mapped from record root level
  - ingestion_ts is timezone-aware
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'ingestion'))

from extract_ponctuality import extract_punctuality, load_config


class TestExtractPunctuality:

    @patch('extract_ponctuality.load_config')
    @patch('extract_ponctuality.ODSv2Client')
    def test_extract_punctuality_success(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        mock_ods_response_punctuality,
        tmp_output_dir
    ):
        """Successful extraction: JSON file created with correct content and field mapping."""
        mock_load_config.return_value = mock_config

        mock_client = MagicMock()
        mock_client.get_all_records.return_value = mock_ods_response_punctuality['results']
        mock_client_class.return_value = mock_client

        extract_punctuality(
            start_date='2024-01-01',
            end_date='2024-01-31',
            output_dir=str(tmp_output_dir)
        )

        mock_client.get_all_records.assert_called_once()
        # extract_fields NOT called — fields read from root directly
        mock_client.extract_fields.assert_not_called()

        output_files = list(tmp_output_dir.glob('*.json'))
        assert len(output_files) == 1
        assert 'punctuality' in output_files[0].name

        with open(output_files[0]) as f:
            data = json.load(f)

        assert len(data) == 2
        assert 'ingestion_ts' in data[0]
        assert data[0]['source'] == 'transilien_punctuality'
        # Fields correctly mapped
        assert data[0]['month'] == '2024-01'
        assert data[0]['line_id'] == 'A'
        assert data[0]['line_name'] == 'RER A'
        assert data[0]['punctuality_rate'] == 95.5

    @patch('extract_ponctuality.load_config')
    @patch('extract_ponctuality.ODSv2Client')
    def test_extract_punctuality_no_records(
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

        extract_punctuality(
            start_date='2024-01-01',
            end_date='2024-01-31',
            output_dir=str(tmp_output_dir)
        )

        assert len(list(tmp_output_dir.glob('*.json'))) == 0

    @patch('extract_ponctuality.load_config')
    @patch('extract_ponctuality.ODSv2Client')
    def test_date_truncation_to_month(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        tmp_output_dir
    ):
        """YYYY-MM-DD input is truncated to YYYY-MM before being sent to the API."""
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = []
        mock_client_class.return_value = mock_client

        extract_punctuality(
            start_date='2024-01-15',   # Day part should be ignored
            end_date='2024-03-20',     # Day part should be ignored
            output_dir=str(tmp_output_dir)
        )

        # WHERE clause must contain YYYY-MM, not YYYY-MM-DD
        call_kwargs = mock_client.get_all_records.call_args[1]
        where = call_kwargs['where']
        assert '2024-01' in where
        assert '2024-03' in where
        assert '2024-01-15' not in where
        assert '2024-03-20' not in where

    @patch('extract_ponctuality.load_config')
    @patch('extract_ponctuality.ODSv2Client')
    def test_ingestion_ts_is_timezone_aware(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        mock_ods_response_punctuality,
        tmp_output_dir
    ):
        """ingestion_ts must be timezone-aware (datetime.now(timezone.utc), not deprecated utcnow())."""
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = mock_ods_response_punctuality['results']
        mock_client_class.return_value = mock_client

        extract_punctuality(
            start_date='2024-01',
            end_date='2024-01',
            output_dir=str(tmp_output_dir)
        )

        output_files = list(tmp_output_dir.glob('*.json'))
        with open(output_files[0]) as f:
            data = json.load(f)

        assert '+00:00' in data[0]['ingestion_ts'] or 'Z' in data[0]['ingestion_ts']

    def test_load_config_is_callable(self):
        """load_config is a callable function."""
        assert callable(load_config)
