"""
HTTP Client for Opendatasoft V2 API

Handles pagination, automatic retry, rate limiting, and field extraction
for interacting with Opendatasoft public data APIs.

API Reference: https://help.opendatasoft.com/apis/ods-search-v2/
"""

import logging
import time
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


class ODSv2Client:
    """
    Client for Opendatasoft API V2.1
    
    Features:
    - Automatic pagination (API limit: 100 records per request)
    - Retry with exponential backoff on failures
    - Rate limiting to avoid API throttling
    - Field extraction and renaming via mapping
    """
    
    def __init__(
        self,
        base_url: str,
        dataset_id: str,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_delay: float = 0.5
    ):
        """
        Initialize Opendatasoft API client
        
        Args:
            base_url: API base URL (e.g., https://data.iledefrance-mobilites.fr/api/explore/v2.1)
            dataset_id: Dataset identifier
            timeout: Request timeout in seconds
            max_retries: Number of retry attempts on failure
            rate_limit_delay: Delay between requests in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.dataset_id = dataset_id
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        
        # HTTP session with automatic retry
        self.session = self._create_session(max_retries)
        
        # Dataset records endpoint
        self.dataset_url = f"{self.base_url}/catalog/datasets/{self.dataset_id}/records"
        
        logger.info(f"ODSv2Client initialized for dataset: {dataset_id}")
    
    def _create_session(self, max_retries: int) -> requests.Session:
        """Create HTTP session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1  # Wait 1s, 2s, 4s... between retries
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def get_records(
        self,
        limit: int = 100,
        offset: int = 0,
        where: Optional[str] = None,
        select: Optional[str] = None,
        order_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch a single page of results
        
        Args:
            limit: Number of results per page (max 100)
            offset: Offset for pagination
            where: WHERE clause filter (e.g., "date >= '2024-01-01'")
            select: Fields to return (e.g., "date, line_code")
            order_by: Sort order (e.g., "date ASC")
        
        Returns:
            API JSON response
        """
        params = {
            'limit': min(limit, 100),  # API enforces max 100
            'offset': offset
        }
        
        if where:
            params['where'] = where
        if select:
            params['select'] = select
        if order_by:
            params['order_by'] = order_by
        
        try:
            logger.debug(f"GET {self.dataset_url} with params: {params}")
            
            response = self.session.get(
                self.dataset_url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Rate limiting - avoid overwhelming API
            time.sleep(self.rate_limit_delay)
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            logger.error(f"Response: {e.response.text if e.response else 'No response'}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def get_all_records(
        self,
        where: Optional[str] = None,
        select: Optional[str] = None,
        order_by: Optional[str] = None,
        max_records: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL results with automatic pagination
        
        Handles pagination transparently - keeps fetching pages until
        all records are retrieved or max_records limit is reached.
        
        Args:
            where: WHERE clause filter
            select: Fields to return
            order_by: Sort order
            max_records: Maximum total records to fetch (None = unlimited)
        
        Returns:
            List of all records
        """
        all_records = []
        offset = 0
        page_size = 100
        total_count = None
        
        logger.info(f"Fetching all records (max: {max_records or 'unlimited'})")
        
        while True:
            # Check if we've reached the requested limit
            if max_records and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                break
            
            # Fetch one page
            response = self.get_records(
                limit=page_size,
                offset=offset,
                where=where,
                select=select,
                order_by=order_by
            )
            
            # Parse response
            records = response.get('results', [])
            if total_count is None:
                total_count = response.get('total_count', 0)
                logger.info(f"Total records available: {total_count}")
            
            if not records:
                logger.info("No more records to fetch")
                break
            
            all_records.extend(records)
            logger.info(f"Fetched {len(all_records)}/{total_count} records")
            
            # Check if we've fetched everything
            if len(all_records) >= total_count:
                break
            
            offset += page_size
        
        logger.info(f"Total records fetched: {len(all_records)}")
        return all_records
    
    def extract_fields(
        self,
        records: List[Dict[str, Any]],
        field_mapping: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        Extract and rename fields from API records
        
        ODS API returns data in record['record']['fields'] structure.
        This method extracts specified fields and renames them.
        
        Args:
            records: Raw records from API
            field_mapping: Mapping {target_name: source_name}
                Example: {'date': 'jour', 'line': 'ligne'}
        
        Returns:
            List of dictionaries with extracted and renamed fields
        """
        extracted = []
        
        for record in records:
            # Data is nested in record['record']['fields']
            fields = record.get('record', {}).get('fields', {})
            
            # Extract only requested fields
            extracted_record = {}
            for target_field, source_field in field_mapping.items():
                # Support dot notation for nested fields
                value = self._get_nested_field(fields, source_field)
                extracted_record[target_field] = value
            
            extracted.append(extracted_record)
        
        return extracted
    
    def _get_nested_field(self, data: Dict, field_path: str) -> Any:
        """
        Retrieve nested field using dot notation
        
        Example: 'address.city' retrieves data['address']['city']
        """
        keys = field_path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
            
            if value is None:
                return None
        
        return value
    
    def get_dataset_info(self) -> Dict[str, Any]:
        """Fetch dataset metadata"""
        url = f"{self.base_url}/catalog/datasets/{self.dataset_id}"
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get dataset info: {e}")
            raise


class ODSv2ClientError(Exception):
    """Exception for ODS client errors"""
    pass
