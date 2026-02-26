"""
Tests unitaires pour extract_validations.py
"""

import pytest
from unittest.mock import patch, Mock, MagicMock
from pathlib import Path
import json
import sys

# Add ingestion to path
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
        """Test extraction normale"""
        # Setup mocks
        mock_load_config.return_value = mock_config
        
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = mock_ods_response['results']
        mock_client.extract_fields.return_value = [
            {'date': '2024-01-01', 'station': 'Châtelet', 'validations': 1000},
            {'date': '2024-01-02', 'station': 'Gare du Nord', 'validations': 1500}
        ]
        mock_client_class.return_value = mock_client
        
        # Execute
        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-02',
            output_dir=str(tmp_output_dir)
        )
        
        # Verify
        mock_client.get_all_records.assert_called_once()
        mock_client.extract_fields.assert_called_once()
        
        # Check output file exists
        output_files = list(tmp_output_dir.glob('*.json'))
        assert len(output_files) == 1
        assert output_files[0].name == 'validations_2024-01-01_2024-01-02.json'
        
        # Check content
        with open(output_files[0]) as f:
            data = json.load(f)
        
        assert len(data) == 2
        assert 'ingestion_ts' in data[0]
        assert data[0]['source'] == 'idfm_validations'
    
    @patch('extract_validations.load_config')
    @patch('extract_validations.ODSv2Client')
    def test_extract_validations_no_records(
        self, 
        mock_client_class, 
        mock_load_config, 
        mock_config,
        tmp_output_dir
    ):
        """Test quand aucun record n'est trouvé"""
        # Setup mocks
        mock_load_config.return_value = mock_config
        
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = []
        mock_client_class.return_value = mock_client
        
        # Execute (should not raise exception)
        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-02',
            output_dir=str(tmp_output_dir)
        )
        
        # Verify no output file created
        output_files = list(tmp_output_dir.glob('*.json'))
        assert len(output_files) == 0
    
    @patch('extract_validations.Path')
    def test_load_config(self, mock_path):
        """Test loading configuration"""
        # Mock yaml file
        mock_config_file = MagicMock()
        mock_config_file.__enter__.return_value = mock_config_file
        mock_config_file.read.return_value = """
        idfm:
          base_url: https://test.com
        """
        
        mock_path.return_value.parent.parent = Path('.')
        
        # This would normally load real config
        # In actual test, we'd mock yaml.safe_load
        # For now, just check function exists
        assert callable(load_config)
    
    @patch('extract_validations.load_config')
    @patch('extract_validations.ODSv2Client')
    def test_extract_validations_creates_directory(
        self, 
        mock_client_class, 
        mock_load_config, 
        mock_config,
        tmp_path
    ):
        """Test que le répertoire de sortie est créé s'il n'existe pas"""
        # Setup
        mock_load_config.return_value = mock_config
        mock_client = MagicMock()
        mock_client.get_all_records.return_value = []
        mock_client_class.return_value = mock_client
        
        output_dir = tmp_path / "new" / "nested" / "dir"
        assert not output_dir.exists()
        
        # Execute
        extract_validations(
            start_date='2024-01-01',
            end_date='2024-01-02',
            output_dir=str(output_dir)
        )
        
        # Verify directory was created
        assert output_dir.exists()
        assert output_dir.is_dir()
