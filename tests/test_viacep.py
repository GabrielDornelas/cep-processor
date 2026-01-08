"""
Unit tests for ViaCEP client module
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import requests

from src.processors.viacep_client import ViaCEPClient


@pytest.fixture
def mock_config():
    """Fixture to provide a mock config for all tests"""
    mock = Mock()
    mock.get_viacep_base_url.return_value = "https://viacep.com.br/ws"
    mock.get_request_timeout.return_value = 30
    mock.get_retry_attempts.return_value = 3
    mock.get_errors_csv_path.return_value = Path("data/viacep_errors.csv")
    return mock


@pytest.fixture
def mock_get_config(mock_config):
    """Fixture to patch get_config and return mock_config"""
    with patch('src.processors.viacep_client.get_config', return_value=mock_config) as mock:
        yield mock


class TestViaCEPClient:
    """Test cases for ViaCEPClient class"""

    def test_init(self, mock_get_config):
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
    def test_query_cep_success(self, mock_session_class, mock_get_config):
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
    def test_query_cep_not_found(self, mock_session_class, mock_get_config):
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
    def test_query_cep_timeout_retry(self, mock_session_class, mock_get_config):
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
    def test_query_cep_max_retries(self, mock_session_class, mock_get_config):
        """Test max retries reached"""
        mock_session = Mock()
        mock_session.headers = {}
        mock_session.get.side_effect = requests.Timeout("Connection timeout")
        mock_session_class.return_value = mock_session
        
        client = ViaCEPClient(retry_attempts=2, retry_delay=0.1)
        result = client.query_cep("01310100")
        
        assert result is None
        assert mock_session.get.call_count == 2

    def test_is_valid_response(self, mock_get_config):
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

