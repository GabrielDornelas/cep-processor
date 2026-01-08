"""
ViaCEP API client module for querying CEP details
"""

import time
from pathlib import Path
from typing import Optional, Dict, Any

import requests

from src.utils.logger import setup_logger
from src.utils.error_handler import ErrorHandler
from src.utils.config_helper import get_config


class ViaCEPClient:
    """
    Client for querying CEP details from ViaCEP API.
    Handles API requests with error handling and retries.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
        retry_attempts: Optional[int] = None,
        retry_delay: float = 1.0,
        errors_csv_path: Optional[Path] = None
    ):
        """
        Initialize the ViaCEP client.

        Args:
            base_url: Base URL for ViaCEP API (optional, will use ConfigHelper if not provided)
            timeout: Request timeout in seconds (optional, will use ConfigHelper if not provided)
            retry_attempts: Number of retry attempts for failed requests (optional, will use ConfigHelper if not provided)
            retry_delay: Delay between retries in seconds
            errors_csv_path: Path to CSV file for storing errors. If None, will use ConfigHelper.
                            Default: data/viacep_errors.csv
        """
        config = get_config()
        self.base_url = (base_url or config.get_viacep_base_url()).rstrip('/')
        self.timeout = timeout or config.get_request_timeout()
        self.retry_attempts = retry_attempts or config.get_retry_attempts()
        self.retry_delay = retry_delay
        self.logger = setup_logger(name="viacep_client")
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CEP-Processor/1.0',
            'Accept': 'application/json'
        })
        
        # Initialize error handler
        if errors_csv_path is None:
            errors_csv_path = config.get_errors_csv_path()
        self.error_handler = ErrorHandler(errors_csv_path=errors_csv_path)

    def query_cep(self, cep: str) -> Optional[Dict[str, Any]]:
        """
        Query CEP details from ViaCEP API.
        Assumes CEP is already in correct format (8 digits, no hyphen).

        Args:
            cep: CEP to query (8 digits, already validated from CSV)

        Returns:
            Dictionary with CEP details or None if not found/error
            Structure: {
                'cep': str,
                'logradouro': str,
                'complemento': str,
                'bairro': str,
                'localidade': str,
                'uf': str,
                'ibge': str,
                'gia': str,
                'ddd': str,
                'siafi': str,
                'erro': bool (if CEP not found)
            }
        """
        url = f"{self.base_url}/{cep}/json/"

        for attempt in range(1, self.retry_attempts + 1):
            try:
                self.logger.debug(f"Querying CEP {cep} (attempt {attempt}/{self.retry_attempts})")
                
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()

                data = response.json()

                # Check if CEP was found
                if isinstance(data, dict) and data.get('erro'):
                    error_msg = f"CEP {cep} not found in ViaCEP"
                    self.logger.warning(error_msg)
                    # Record error to CSV (CEP not found is a valid error to record)
                    self.error_handler.record_api_error(
                        cep=cep,
                        error_message=error_msg,
                        status_code=200  # API returns 200 but with erro: true
                    )
                    return None

                # Validate response structure
                if not isinstance(data, dict):
                    error_msg = f"Invalid response format for CEP {cep}"
                    self.logger.error(error_msg)
                    # Record error to CSV (only on last attempt)
                    if attempt == self.retry_attempts:
                        self.error_handler.record_api_error(
                            cep=cep,
                            error_message=error_msg
                        )
                    return None

                self.logger.debug(f"Successfully queried CEP {cep}")
                return data

            except requests.Timeout:
                error_msg = f"Timeout querying CEP {cep} (attempt {attempt}/{self.retry_attempts})"
                self.logger.warning(error_msg)
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay * attempt)  # Exponential backoff
                else:
                    self.logger.error(f"Max retries reached for CEP {cep}")
                    # Record error to CSV on final failure
                    self.error_handler.record_api_error(
                        cep=cep,
                        error_message=f"Timeout after {self.retry_attempts} attempts",
                        retry_attempt=attempt
                    )
                    return None

            except requests.RequestException as e:
                error_msg = f"Error querying CEP {cep}: {e} (attempt {attempt}/{self.retry_attempts})"
                self.logger.error(error_msg)
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay * attempt)  # Exponential backoff
                else:
                    self.logger.error(f"Max retries reached for CEP {cep}")
                    # Record error to CSV on final failure
                    status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                    self.error_handler.record_api_error(
                        cep=cep,
                        error_message=str(e),
                        status_code=status_code,
                        retry_attempt=attempt
                    )
                    return None

            except ValueError as e:
                error_msg = f"Error parsing JSON response for CEP {cep}: {e}"
                self.logger.error(error_msg)
                # Record error to CSV (only on last attempt)
                if attempt == self.retry_attempts:
                    self.error_handler.record_api_error(
                        cep=cep,
                        error_message=error_msg
                    )
                return None

        return None

    def query_multiple_ceps(self, ceps: list[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Query multiple CEPs (sequential, with delay between requests).

        Args:
            ceps: List of CEPs to query

        Returns:
            Dictionary mapping CEP -> result (or None if error/not found)
        """
        results = {}
        
        for i, cep in enumerate(ceps, 1):
            self.logger.info(f"Querying CEP {i}/{len(ceps)}: {cep}")
            result = self.query_cep(cep)
            results[cep] = result
            
            # Small delay between requests to respect rate limits
            if i < len(ceps):
                time.sleep(0.2)  # 200ms delay
        
        return results

    def is_valid_response(self, data: Dict[str, Any]) -> bool:
        """
        Check if ViaCEP response is valid and contains CEP data.

        Args:
            data: Response data from ViaCEP API

        Returns:
            True if valid response with CEP data, False otherwise
        """
        if not isinstance(data, dict):
            return False
        
        # Check if CEP was not found
        if data.get('erro'):
            return False
        
        # Check if required fields are present
        required_fields = ['cep', 'localidade', 'uf']
        return all(field in data for field in required_fields)

