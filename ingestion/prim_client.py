"""
HTTP client for PRIM APIs.

This client is intentionally generic: it targets a single fully qualified API URL
and supports API key authentication either in query parameters or headers.
"""

import logging
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class PRIMClient:
    """Small HTTP client for PRIM JSON endpoints."""

    def __init__(
        self,
        url: str,
        api_key: str,
        api_key_location: str = "query",
        api_key_name: str = "apikey",
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_delay: float = 0.5,
        default_params: Optional[Dict[str, Any]] = None,
    ):
        self.url = url
        self.api_key = api_key
        self.api_key_location = api_key_location
        self.api_key_name = api_key_name
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.default_params = default_params or {}
        self.session = self._create_session(max_retries)

        logger.info("PRIMClient initialized for %s", url)

    def _create_session(self, max_retries: int) -> requests.Session:
        session = requests.Session()

        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _build_auth(self) -> tuple[dict[str, str], dict[str, str]]:
        params: dict[str, str] = {}
        headers: dict[str, str] = {"Accept": "application/json"}

        if self.api_key_location == "query":
            params[self.api_key_name] = self.api_key
        elif self.api_key_location == "header":
            headers[self.api_key_name] = self.api_key
        elif self.api_key_location == "bearer":
            headers["Authorization"] = f"Bearer {self.api_key}"
        else:
            raise ValueError(
                f"Unsupported PRIM api_key_location: {self.api_key_location}"
            )

        return params, headers

    def get_json(
        self,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any] | list[Dict[str, Any]]:
        auth_params, auth_headers = self._build_auth()

        request_params = {**self.default_params, **auth_params, **(params or {})}
        request_headers = {**auth_headers, **(headers or {})}

        try:
            response = self.session.get(
                self.url,
                params=request_params,
                headers=request_headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            time.sleep(self.rate_limit_delay)
            return response.json()
        except requests.exceptions.RequestException as exc:
            logger.error("PRIM request failed: %s", exc)
            raise


class PRIMClientError(Exception):
    """Exception for PRIM client errors."""

    pass
