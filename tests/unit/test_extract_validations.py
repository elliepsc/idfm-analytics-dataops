"""
Unit tests for extract_validations.py
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "ingestion"))

from extract_validations import extract_validations, load_config


class TestExtractValidations:

    @patch("extract_validations.load_config")
    @patch("extract_validations.ODSv2Client")
    def test_extract_validations_success(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        mock_ods_response,
        tmp_output_dir,
    ):
        """Successful extraction: JSON file created with correct content."""
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = mock_ods_response["results"]
        mock_client_class.return_value = mock_client

        extract_validations(
            start_date="2024-01-01",
            end_date="2024-01-31",
            output_dir=str(tmp_output_dir),
        )

        output_files = list(tmp_output_dir.glob("*.json"))
        assert len(output_files) == 1

        with open(output_files[0]) as f:
            data = json.load(f)

        assert len(data) == 2
        assert "ingestion_ts" in data[0]
        assert data[0]["source"] == "idfm_validations_rail"

    @patch("extract_validations.load_config")
    @patch("extract_validations.ODSv2Client")
    def test_extract_validations_no_records(
        self,
        mock_client_class,
        mock_load_config,
        mock_config,
        tmp_output_dir,
    ):
        """No records returned → no file created."""
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = []
        mock_client_class.return_value = mock_client

        extract_validations(
            start_date="2024-01-01",
            end_date="2024-01-31",
            output_dir=str(tmp_output_dir),
        )

        assert len(list(tmp_output_dir.glob("*.json"))) == 0

    def test_load_config_is_callable(self):
        """load_config is a callable function."""
        assert callable(load_config)
