"""
Configuration helper for managing environment variables across different environments
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

from src.utils.logger import setup_logger


class ConfigHelper:
    """
    Helper class for managing configuration across different environments.
    Supports: local, staging, production
    """

    def __init__(self, env: Optional[str] = None):
        """
        Initialize configuration helper.

        Args:
            env: Environment name ('local', 'staging', 'production').
                 If None, will be detected from ENV environment variable or default to 'local'
        """
        self.logger = setup_logger(name="config_helper")
        self.project_root = Path(__file__).parent.parent.parent
        
        # Detect environment
        self.env = env or os.getenv('ENV', 'local').lower()
        
        if self.env not in ['local', 'staging', 'production']:
            self.logger.warning(f"Unknown environment '{self.env}', defaulting to 'local'")
            self.env = 'local'
        
        # Load environment variables
        self._load_env_files()
        
        self.logger.info(f"Configuration initialized for environment: {self.env}")

    def _load_env_files(self):
        """Load environment variables from .env files in priority order."""
        # Skip loading if SKIP_ENV_LOAD is set
        if os.getenv('SKIP_ENV_LOAD'):
            self.logger.debug("Skipping .env file loading (SKIP_ENV_LOAD set)")
            return
        
        # Priority order: .env.{env}, .env.local, .env
        env_files = [
            self.project_root / f'.env.{self.env}',
            self.project_root / '.env.local',
            self.project_root / '.env'
        ]
        
        # Load in reverse order so later files override earlier ones
        for env_file in reversed(env_files):
            if env_file.exists():
                self.logger.debug(f"Loading environment file: {env_file}")
                load_dotenv(env_file, override=False)  # Don't override already set vars
            else:
                self.logger.debug(f"Environment file not found: {env_file}")

    def get(self, key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
        """
        Get environment variable value.

        Args:
            key: Environment variable name
            default: Default value if not found
            required: If True, raise error if variable is not set

        Returns:
            Environment variable value or default

        Raises:
            ValueError: If required=True and variable is not set
        """
        value = os.getenv(key, default)
        
        if required and value is None:
            raise ValueError(f"Required environment variable '{key}' is not set")
        
        return value

    def get_database_url(self) -> str:
        """
        Get PostgreSQL database URL.

        Returns:
            Database connection URL
        """
        # Try DATABASE_URL first, then construct from components
        database_url = self.get('DATABASE_URL')
        
        if database_url:
            return database_url
        
        # Construct from components
        host = self.get('POSTGRES_HOST', 'localhost')
        port = self.get('POSTGRES_PORT', '5432')
        user = self.get('POSTGRES_USER', 'cep_user')
        password = self.get('POSTGRES_PASSWORD', 'cep_password')
        database = self.get('POSTGRES_DB', 'cep_processor')
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def get_rabbitmq_url(self) -> str:
        """
        Get RabbitMQ connection URL.

        Returns:
            RabbitMQ connection URL
        """
        # Try RABBITMQ_URL first, then construct from components
        rabbitmq_url = self.get('RABBITMQ_URL')
        
        if rabbitmq_url:
            return rabbitmq_url
        
        # Construct from components
        host = self.get('RABBITMQ_HOST', 'localhost')
        port = self.get('RABBITMQ_PORT', '5672')
        user = self.get('RABBITMQ_USER', 'guest')
        password = self.get('RABBITMQ_PASSWORD', 'guest')
        
        return f"amqp://{user}:{password}@{host}:{port}/"

    def get_viacep_base_url(self) -> str:
        """Get ViaCEP API base URL."""
        return self.get('VIACEP_BASE_URL', 'https://viacep.com.br/ws')

    def get_rate_limit_per_second(self) -> float:
        """Get rate limit per second for API requests."""
        return float(self.get('RATE_LIMIT_PER_SECOND', '5.0'))

    def get_scraping_base_url(self) -> str:
        """Get web scraping base URL."""
        return self.get(
            'SCRAPING_BASE_URL',
            'https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/'
        )

    def get_scraping_delay(self) -> float:
        """Get delay between scraping requests in seconds."""
        return float(self.get('SCRAPING_DELAY', '2.0'))

    def get_max_ceps(self) -> int:
        """Get maximum number of CEPs to process."""
        return int(self.get('MAX_CEPS', '10000'))

    def get_request_timeout(self) -> int:
        """Get request timeout in seconds."""
        return int(self.get('REQUEST_TIMEOUT', '30'))

    def get_retry_attempts(self) -> int:
        """Get number of retry attempts for failed requests."""
        return int(self.get('RETRY_ATTEMPTS', '3'))

    def get_log_level(self) -> str:
        """Get logging level."""
        return self.get('LOG_LEVEL', 'INFO').upper()

    def get_ceps_csv_path(self) -> Path:
        """Get path to CEPs CSV file."""
        path_str = self.get('CEPS_CSV_PATH', 'data/ceps_collected.csv')
        if Path(path_str).is_absolute():
            return Path(path_str)
        return self.project_root / path_str

    def get_errors_csv_path(self) -> Path:
        """Get path to errors CSV file."""
        path_str = self.get('ERRORS_CSV_PATH', 'data/viacep_errors.csv')
        if Path(path_str).is_absolute():
            return Path(path_str)
        return self.project_root / path_str

    def is_docker(self) -> bool:
        """Check if running inside Docker container."""
        return os.path.exists('/.dockerenv') or os.getenv('DOCKER_CONTAINER') == 'true'

    def get_environment(self) -> str:
        """Get current environment name."""
        return self.env


# Global config instance
_config: Optional[ConfigHelper] = None
_config_env: Optional[str] = None


def get_config(env: Optional[str] = None, force_reload: bool = False) -> ConfigHelper:
    """
    Get global configuration instance (singleton).

    Args:
        env: Environment name (only used on first call or if force_reload=True)
        force_reload: Force reload of configuration even if already initialized

    Returns:
        ConfigHelper instance
    """
    global _config, _config_env
    
    # If environment changed or force reload, recreate config
    if force_reload or _config is None or (env and _config_env != env):
        _config = ConfigHelper(env=env)
        _config_env = env or os.getenv('ENV', 'local').lower()
    
    return _config
