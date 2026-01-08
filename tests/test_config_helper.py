"""
Unit tests for config helper module
"""

import pytest
import os
from unittest.mock import patch

from src.utils.config_helper import ConfigHelper, get_config


class TestConfigHelper:
    """Test cases for ConfigHelper class"""

    def test_init_default(self):
        """Test ConfigHelper initialization with default environment"""
        with patch.dict(os.environ, {}, clear=True):
            config = ConfigHelper()
            assert config.env == 'local'

    def test_init_with_env(self):
        """Test ConfigHelper initialization with specific environment"""
        config = ConfigHelper(env='staging')
        assert config.env == 'staging'

    def test_get_existing_var(self):
        """Test getting existing environment variable"""
        with patch.dict(os.environ, {'TEST_VAR': 'test_value'}, clear=False):
            config = ConfigHelper()
            assert config.get('TEST_VAR') == 'test_value'

    def test_get_with_default(self):
        """Test getting variable with default value"""
        with patch.dict(os.environ, {}, clear=True):
            config = ConfigHelper()
            assert config.get('NON_EXISTENT', 'default_value') == 'default_value'

    def test_get_required_missing(self):
        """Test getting required variable that doesn't exist"""
        with patch.dict(os.environ, {}, clear=True):
            config = ConfigHelper()
            with pytest.raises(ValueError, match="Required environment variable"):
                config.get('REQUIRED_VAR', required=True)

    def test_get_database_url_from_components(self):
        """Test constructing database URL from components"""
        # Clear existing database-related env vars first
        env_vars_to_clear = ['DATABASE_URL', 'POSTGRES_HOST', 'POSTGRES_PORT', 
                            'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB']
        for var in env_vars_to_clear:
            os.environ.pop(var, None)
        
        with patch.dict(os.environ, {
            'SKIP_ENV_LOAD': 'true',  # Skip loading .env files
            'POSTGRES_HOST': 'test_host',
            'POSTGRES_PORT': '5433',
            'POSTGRES_USER': 'test_user',
            'POSTGRES_PASSWORD': 'test_pass',
            'POSTGRES_DB': 'test_db'
        }, clear=False):
            config = ConfigHelper()
            url = config.get_database_url()
            assert 'test_user' in url
            assert 'test_pass' in url
            assert 'test_host' in url
            assert '5433' in url
            assert 'test_db' in url

    def test_get_database_url_from_env(self):
        """Test getting database URL from DATABASE_URL env var"""
        with patch.dict(os.environ, {
            'DATABASE_URL': 'postgresql://user:pass@host:5432/db'
        }, clear=False):
            config = ConfigHelper()
            url = config.get_database_url()
            assert url == 'postgresql://user:pass@host:5432/db'

    def test_get_rabbitmq_url_from_components(self):
        """Test constructing RabbitMQ URL from components"""
        # Clear existing RabbitMQ-related env vars first
        env_vars_to_clear = ['RABBITMQ_URL', 'RABBITMQ_HOST', 'RABBITMQ_PORT', 
                            'RABBITMQ_USER', 'RABBITMQ_PASSWORD']
        for var in env_vars_to_clear:
            os.environ.pop(var, None)
        
        with patch.dict(os.environ, {
            'SKIP_ENV_LOAD': 'true',
            'RABBITMQ_HOST': 'test_host',
            'RABBITMQ_PORT': '5673',
            'RABBITMQ_USER': 'test_user',
            'RABBITMQ_PASSWORD': 'test_pass'
        }, clear=False):
            config = ConfigHelper()
            url = config.get_rabbitmq_url()
            assert 'test_user' in url
            assert 'test_pass' in url
            assert 'test_host' in url
            assert '5673' in url

    def test_get_rabbitmq_url_from_env(self):
        """Test getting RabbitMQ URL from RABBITMQ_URL env var"""
        with patch.dict(os.environ, {
            'RABBITMQ_URL': 'amqp://user:pass@host:5672/'
        }, clear=False):
            config = ConfigHelper()
            url = config.get_rabbitmq_url()
            assert url == 'amqp://user:pass@host:5672/'

    def test_get_viacep_base_url(self):
        """Test getting ViaCEP base URL"""
        with patch.dict(os.environ, {
            'VIACEP_BASE_URL': 'https://custom-viacep.com/ws'
        }, clear=False):
            config = ConfigHelper()
            assert config.get_viacep_base_url() == 'https://custom-viacep.com/ws'

    def test_get_rate_limit_per_second(self):
        """Test getting rate limit"""
        with patch.dict(os.environ, {
            'RATE_LIMIT_PER_SECOND': '10'
        }, clear=False):
            config = ConfigHelper()
            assert config.get_rate_limit_per_second() == 10.0

    def test_get_max_ceps(self):
        """Test getting max CEPs"""
        with patch.dict(os.environ, {
            'MAX_CEPS': '5000'
        }, clear=False):
            config = ConfigHelper()
            assert config.get_max_ceps() == 5000

    def test_get_ceps_csv_path(self):
        """Test getting CEPs CSV path"""
        with patch.dict(os.environ, {
            'CEPS_CSV_PATH': 'custom/path/ceps.csv'
        }, clear=False):
            config = ConfigHelper()
            path = config.get_ceps_csv_path()
            assert 'ceps.csv' in str(path)

    def test_get_errors_csv_path(self):
        """Test getting errors CSV path"""
        with patch.dict(os.environ, {
            'ERRORS_CSV_PATH': 'custom/path/errors.csv'
        }, clear=False):
            config = ConfigHelper()
            path = config.get_errors_csv_path()
            assert 'errors.csv' in str(path)

    def test_is_docker(self):
        """Test Docker detection"""
        config = ConfigHelper()
        # This will depend on actual environment, so just test it doesn't crash
        result = config.is_docker()
        assert isinstance(result, bool)

    def test_get_environment(self):
        """Test getting environment name"""
        config = ConfigHelper(env='staging')
        assert config.get_environment() == 'staging'

    @patch('src.utils.config_helper.load_dotenv')
    @patch('src.utils.config_helper.Path.exists')
    def test_load_env_files_precedence(self, mock_exists, mock_load_dotenv):
        """Test that .env files are loaded in correct precedence order"""
        # All files exist
        mock_exists.return_value = True
        
        # Create config with staging environment
        config = ConfigHelper(env='staging')
        
        # Verify load_dotenv was called 3 times
        assert mock_load_dotenv.call_count == 3
        
        # Get the project root from the config
        project_root = config.project_root
        
        # Verify order: .env, .env.local, .env.staging
        calls = mock_load_dotenv.call_args_list
        assert str(calls[0][0][0]) == str(project_root / '.env')
        assert calls[0][1]['override'] is True
        assert str(calls[1][0][0]) == str(project_root / '.env.local')
        assert calls[1][1]['override'] is True
        assert str(calls[2][0][0]) == str(project_root / '.env.staging')
        assert calls[2][1]['override'] is True


class TestGetConfig:
    """Test cases for get_config function"""

    def test_get_config_singleton(self):
        """Test that get_config returns singleton"""
        with patch.dict(os.environ, {}, clear=True):
            config1 = get_config()
            config2 = get_config()
            assert config1 is config2

    def test_get_config_with_env(self):
        """Test get_config with environment parameter"""
        with patch.dict(os.environ, {'SKIP_ENV_LOAD': 'true'}, clear=True):
            # Force reload to test with different env
            config = get_config(env='production', force_reload=True)
            assert config.get_environment() == 'production'

