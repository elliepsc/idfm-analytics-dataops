"""
Tests unitaires pour extract_punctuality.py
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import json
import sys

# Add ingestion to path
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
        mock_ods_response,
        tmp_output_dir
    ):
        """Test extraction normale de ponctualité"""
        # Setup mocks
        mock_load_config.return_value = mock_config
        
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = mock_ods_response['results']
        mock_client.extract_fields.return_value = [
            {'date': '2024-01', 'line': 'RER A', 'punctuality_rate': 95.5},
            {'date': '2024-01', 'line': 'RER B', 'punctuality_rate': 92.3}
        ]
        mock_client_class.return_value = mock_client
        
        # Execute
        extract_punctuality(
            start_date='2024-01-01',
            end_date='2024-01-31',
            output_dir=str(tmp_output_dir)
        )
        
        # Verify
        mock_client.get_all_records.assert_called_once()
        mock_client.extract_fields.assert_called_once()
        
        # Check output file
        output_files = list(tmp_output_dir.glob('*.json'))
        assert len(output_files) == 1
        assert 'punctuality' in output_files[0].name
        
        # Check content
        with open(output_files[0]) as f:
            data = json.load(f)
        
        assert len(data) == 2
        assert 'ingestion_ts' in data[0]
        assert data[0]['source'] == 'transilien_punctuality'
    
    @patch('extract_ponctuality.load_config')
    @patch('extract_ponctuality.ODSv2Client')
    def test_extract_punctuality_no_records(
        self, 
        mock_client_class, 
        mock_load_config, 
        mock_config,
        tmp_output_dir
    ):
        """Test quand aucun record de ponctualité trouvé"""
        # Setup
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = []
        mock_client_class.return_value = mock_client
        
        # Execute
        extract_punctuality(
            start_date='2024-01-01',
            end_date='2024-01-31',
            output_dir=str(tmp_output_dir)
        )
        
        # Verify no file created
        output_files = list(tmp_output_dir.glob('*.json'))
        assert len(output_files) == 0
    
    @patch('extract_ponctuality.load_config')
    @patch('extract_ponctuality.ODSv2Client')
    def test_extract_punctuality_field_mapping(
        self, 
        mock_client_class, 
        mock_load_config, 
        mock_config,
        tmp_output_dir
    ):
        """Test que le field mapping est correctement appliqué"""
        # Setup
        mock_load_config.return_value = mock_config
        
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = [{'raw': 'data'}]
        
        # Expected field mapping from config
        expected_mapping = {
            'date': 'date',
            'line': 'ligne',
            'punctuality_rate': 'taux_regularite'
        }
        
        mock_client.extract_fields.return_value = [
            {'date': '2024-01', 'line': 'RER A', 'punctuality_rate': 95.5}
        ]
        mock_client_class.return_value = mock_client
        
        # Execute
        extract_punctuality(
            start_date='2024-01-01',
            end_date='2024-01-31',
            output_dir=str(tmp_output_dir)
        )
        
        # Verify extract_fields was called with correct mapping
        call_args = mock_client.extract_fields.call_args
        actual_mapping = call_args[0][1]  # Second argument
        
        # Check that mapping keys match (order doesn't matter)
        assert set(actual_mapping.keys()) == set(expected_mapping.keys())
    
    def test_load_config_exists(self):
        """Test que la fonction load_config existe"""
        assert callable(load_config)
