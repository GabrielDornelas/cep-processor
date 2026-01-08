"""
Unit tests for ViaCEP client module
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from src.processors.viacep_client import ViaCEPClient


class TestViaCEPClient:
    """Test cases for ViaCEPClient class"""

    def test_init(self):
        """Test ViaCEPClient initialization"""
        client = ViaCEPClient(
            base_url="https://viacep.com.br/ws",
            timeout=30,
            retry_attempts=3
        )
        
        assert client.base_url == "https://viacep.com.br/ws"
        assert client.timeout == 30
        assert client.retry_attempts == 3

    @patch('src.processors.viacep_client.requests.Session')
    def test_query_cep_success(self, mock_session_class):
        """Test successful CEP query"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {
            'cep': '01310-100',
            'logradouro': 'Avenida Paulista',
            'complemento': '',
            'bairro': 'Bela Vista',
            'localidade': 'São Paulo',
            'uf': 'SP',
            'ibge': '3550308',
            'gia': '1004',
            'ddd': '11',
            'siafi': '7107'
        }
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session.headers = {}
        mock_session_class.return_value = mock_session
        
        client = ViaCEPClient()
        result = client.query_cep("01310100")
        
        assert result is not None
        assert result['cep'] == '01310-100'
        assert result['localidade'] == 'São Paulo'
        assert result['uf'] == 'SP'

    @patch('src.processors.viacep_client.requests.Session')
    def test_query_cep_not_found(self, mock_session_class):
        """Test CEP not found (erro: true)"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {'erro': True}
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session.headers = {}
        mock_session_class.return_value = mock_session
        
        client = ViaCEPClient()
        result = client.query_cep("00000000")
        
        assert result is None

    @patch('src.processors.viacep_client.requests.Session')
    def test_query_cep_timeout_retry(self, mock_session_class):
        """Test timeout with retry"""
        mock_session = Mock()
        mock_session.headers = {}
        
        # First call times out, second succeeds
        mock_response = Mock()
        mock_response.json.return_value = {
            'cep': '01310-100',
            'localidade': 'São Paulo',
            'uf': 'SP'
        }
        mock_response.raise_for_status = Mock()
        
        mock_session.get.side_effect = [
            requests.Timeout("Connection timeout"),
            mock_response
        ]
        mock_session_class.return_value = mock_session
        
        client = ViaCEPClient(retry_attempts=2, retry_delay=0.1)
        result = client.query_cep("01310100")
        
        assert result is not None
        assert mock_session.get.call_count == 2

    @patch('src.processors.viacep_client.requests.Session')
    def test_query_cep_max_retries(self, mock_session_class):
        """Test max retries reached"""
        mock_session = Mock()
        mock_session.headers = {}
        mock_session.get.side_effect = requests.Timeout("Connection timeout")
        mock_session_class.return_value = mock_session
        
        client = ViaCEPClient(retry_attempts=2, retry_delay=0.1)
        result = client.query_cep("01310100")
        
        assert result is None
        assert mock_session.get.call_count == 2

    def test_is_valid_response(self):
        """Test response validation"""
        client = ViaCEPClient()
        
        # Valid response
        valid_response = {
            'cep': '01310-100',
            'logradouro': 'Avenida Paulista',
            'bairro': 'Bela Vista',
            'localidade': 'São Paulo',
            'uf': 'SP'
        }
        assert client.is_valid_response(valid_response) is True
        
        # CEP not found
        not_found_response = {'erro': True}
        assert client.is_valid_response(not_found_response) is False
        
        # Missing required fields
        incomplete_response = {'cep': '01310-100'}
        assert client.is_valid_response(incomplete_response) is False
        
        # Invalid type
        assert client.is_valid_response([]) is False
        assert client.is_valid_response(None) is False

