"""
Unit tests for storage module
"""

import pytest
from unittest.mock import Mock, patch

from src.storage.models import CEP
from src.storage.database import DatabaseManager


@pytest.fixture
def mock_config():
    """Fixture to provide a mock config for all tests"""
    mock = Mock()
    mock.get_database_url.return_value = "postgresql://user:pass@localhost:5432/testdb"
    return mock


@pytest.fixture
def mock_get_config(mock_config):
    """Fixture to patch get_config and return mock_config"""
    with patch('src.storage.database.get_config', return_value=mock_config) as mock:
        yield mock


class TestCEPModel:
    """Test cases for CEP model"""

    def test_cep_model_creation(self):
        """Test creating CEP model"""
        cep = CEP(
            cep='01310100',
            logradouro='Avenida Paulista',
            bairro='Bela Vista',
            localidade='São Paulo',
            uf='SP'
        )

        assert cep.cep == '01310100'
        assert cep.logradouro == 'Avenida Paulista'
        assert cep.localidade == 'São Paulo'
        assert cep.uf == 'SP'

    def test_cep_to_dict(self):
        """Test converting CEP to dictionary"""
        cep = CEP(
            cep='01310100',
            logradouro='Avenida Paulista',
            bairro='Bela Vista',
            localidade='São Paulo',
            uf='SP',
            ibge='3550308',
            ddd='11'
        )

        result = cep.to_dict()

        assert result['cep'] == '01310100'
        assert result['logradouro'] == 'Avenida Paulista'
        assert result['localidade'] == 'São Paulo'
        assert result['uf'] == 'SP'
        assert result['ibge'] == '3550308'
        assert result['ddd'] == '11'
        assert 'created_at' in result
        assert 'updated_at' in result

    def test_cep_from_viacep_response(self):
        """Test creating CEP from ViaCEP response"""
        viacep_data = {
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

        cep = CEP.from_viacep_response(viacep_data)

        assert cep.cep == '01310100'  # Hyphen removed
        assert cep.logradouro == 'Avenida Paulista'
        assert cep.localidade == 'São Paulo'
        assert cep.uf == 'SP'
        assert cep.ibge == '3550308'

    def test_cep_from_viacep_response_with_none_values(self):
        """Test creating CEP from ViaCEP response with None values"""
        viacep_data = {
            'cep': '01310100',
            'logradouro': None,
            'complemento': None,
            'bairro': None,
            'localidade': 'São Paulo',
            'uf': 'SP'
        }

        cep = CEP.from_viacep_response(viacep_data)

        assert cep.cep == '01310100'
        assert cep.logradouro is None
        assert cep.complemento is None
        assert cep.bairro is None
        assert cep.localidade == 'São Paulo'


class TestDatabaseManager:
    """Test cases for DatabaseManager"""

    def test_init(self):
        """Test DatabaseManager initialization"""
        db_url = "postgresql://user:pass@localhost:5432/testdb"
        manager = DatabaseManager(db_url)

        assert manager.database_url == db_url
        assert manager.engine is None
        assert manager.SessionLocal is None

    @patch('src.storage.database.create_engine')
    def test_connect_success(self, mock_create_engine, mock_get_config):
        """Test successful database connection"""
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_conn.execute = Mock()
        mock_create_engine.return_value = mock_engine

        manager = DatabaseManager("postgresql://user:pass@localhost:5432/testdb")
        result = manager.connect()

        assert result is True
        assert manager.engine == mock_engine
        mock_create_engine.assert_called_once()

    @patch('src.storage.database.create_engine')
    def test_connect_failure(self, mock_create_engine, mock_get_config):
        """Test failed database connection"""
        from sqlalchemy.exc import SQLAlchemyError
        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")

        manager = DatabaseManager("postgresql://user:pass@localhost:5432/testdb")
        result = manager.connect()

        assert result is False

    def test_mask_url(self):
        """Test URL masking for logging"""
        manager = DatabaseManager("postgresql://user:password@localhost:5432/testdb")
        
        masked = manager._mask_url("postgresql://user:password@localhost:5432/testdb")
        
        assert "password" not in masked
        assert "***" in masked

    @patch('src.storage.database.create_engine')
    @patch('src.storage.database.Base.metadata.create_all')
    def test_create_tables(self, mock_create_all, mock_create_engine, mock_get_config):
        """Test creating database tables"""
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_conn.execute = Mock()
        mock_create_engine.return_value = mock_engine

        manager = DatabaseManager("postgresql://user:pass@localhost:5432/testdb")
        manager.connect()
        result = manager.create_tables()

        assert result is True
        mock_create_all.assert_called_once_with(bind=mock_engine)

    @patch('src.storage.database.create_engine')
    def test_save_cep_new(self, mock_create_engine, mock_get_config):
        """Test saving new CEP"""
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_conn.execute = Mock()
        mock_create_engine.return_value = mock_engine

        manager = DatabaseManager("postgresql://user:pass@localhost:5432/testdb")
        manager.connect()

        viacep_data = {
            'cep': '01310-100',
            'logradouro': 'Avenida Paulista',
            'localidade': 'São Paulo',
            'uf': 'SP'
        }

        with patch.object(manager, 'get_session') as mock_get_session:
            mock_session = Mock()
            mock_session.query.return_value.filter_by.return_value.first.return_value = None
            mock_session.commit = Mock()
            mock_session.close = Mock()
            mock_get_session.return_value.__enter__ = Mock(return_value=mock_session)
            mock_get_session.return_value.__exit__ = Mock(return_value=None)

            result = manager.save_cep(viacep_data)

            assert result is True
            mock_session.add.assert_called_once()

    @patch('src.storage.database.create_engine')
    def test_save_cep_existing(self, mock_create_engine, mock_get_config):
        """Test updating existing CEP"""
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_conn.execute = Mock()
        mock_create_engine.return_value = mock_engine

        manager = DatabaseManager("postgresql://user:pass@localhost:5432/testdb")
        manager.connect()

        viacep_data = {
            'cep': '01310-100',
            'logradouro': 'Avenida Paulista Updated',
            'localidade': 'São Paulo',
            'uf': 'SP'
        }

        existing_cep = CEP(cep='01310100', logradouro='Avenida Paulista', localidade='São Paulo', uf='SP')

        with patch.object(manager, 'get_session') as mock_get_session:
            mock_session = Mock()
            mock_session.query.return_value.filter_by.return_value.first.return_value = existing_cep
            mock_session.commit = Mock()
            mock_session.close = Mock()
            mock_get_session.return_value.__enter__ = Mock(return_value=mock_session)
            mock_get_session.return_value.__exit__ = Mock(return_value=None)

            result = manager.save_cep(viacep_data)

            assert result is True
            assert existing_cep.logradouro == 'Avenida Paulista Updated'

    @patch('src.storage.database.create_engine')
    def test_get_cep(self, mock_create_engine, mock_get_config):
        """Test retrieving CEP from database"""
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_conn.execute = Mock()
        mock_create_engine.return_value = mock_engine

        manager = DatabaseManager("postgresql://user:pass@localhost:5432/testdb")
        manager.connect()

        expected_cep = CEP(cep='01310100', logradouro='Avenida Paulista', localidade='São Paulo', uf='SP')

        with patch.object(manager, 'get_session') as mock_get_session:
            mock_session = Mock()
            mock_session.query.return_value.filter_by.return_value.first.return_value = expected_cep
            mock_session.commit = Mock()
            mock_session.close = Mock()
            mock_get_session.return_value.__enter__ = Mock(return_value=mock_session)
            mock_get_session.return_value.__exit__ = Mock(return_value=None)

            result = manager.get_cep('01310-100')  # With hyphen

            assert result == expected_cep
            assert result.cep == '01310100'

    @patch('src.storage.database.create_engine')
    def test_count_ceps(self, mock_create_engine, mock_get_config):
        """Test counting CEPs in database"""
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        mock_conn.execute = Mock()
        mock_create_engine.return_value = mock_engine

        manager = DatabaseManager("postgresql://user:pass@localhost:5432/testdb")
        manager.connect()

        with patch.object(manager, 'get_session') as mock_get_session:
            mock_session = Mock()
            mock_session.query.return_value.count.return_value = 42
            mock_session.commit = Mock()
            mock_session.close = Mock()
            mock_get_session.return_value.__enter__ = Mock(return_value=mock_session)
            mock_get_session.return_value.__exit__ = Mock(return_value=None)

            count = manager.count_ceps()

            assert count == 42

