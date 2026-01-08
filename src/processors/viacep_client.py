"""
ViaCEP API client module for querying CEP details
"""

import time
from typing import Optional, Dict, Any
from urllib.parse import urljoin

import requests

from src.utils.logger import setup_logger


class ViaCEPClient:
    """
    Client for querying CEP details from ViaCEP API.
    Handles API requests with error handling and retries.
    """

    def __init__(
        self,
        base_url: str = "https://viacep.com.br/ws",
        timeout: int = 30,
        retry_attempts: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the ViaCEP client.

        Args:
            base_url: Base URL for ViaCEP API
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts for failed requests
            retry_delay: Delay between retries in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.logger = setup_logger(name="viacep_client")
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CEP-Processor/1.0',
            'Accept': 'application/json'
        })

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
                    self.logger.warning(f"CEP {cep} not found in ViaCEP")
                    return None

                # Validate response structure
                if not isinstance(data, dict):
                    self.logger.error(f"Invalid response format for CEP {cep}")
                    return None

                self.logger.debug(f"Successfully queried CEP {cep}")
                return data

            except requests.Timeout:
                self.logger.warning(f"Timeout querying CEP {cep} (attempt {attempt}/{self.retry_attempts})")
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay * attempt)  # Exponential backoff
                else:
                    self.logger.error(f"Max retries reached for CEP {cep}")
                    return None

            except requests.RequestException as e:
                self.logger.error(f"Error querying CEP {cep}: {e} (attempt {attempt}/{self.retry_attempts})")
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay * attempt)  # Exponential backoff
                else:
                    self.logger.error(f"Max retries reached for CEP {cep}")
                    return None

            except ValueError as e:
                self.logger.error(f"Error parsing JSON response for CEP {cep}: {e}")
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

