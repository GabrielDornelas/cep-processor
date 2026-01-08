"""
Unit tests for exporters module
"""

import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime, timezone
from xml.etree.ElementTree import fromstring

from src.storage.models import CEP
from src.exporters.json_exporter import JSONExporter
from src.exporters.xml_exporter import XMLExporter




@pytest.fixture
def sample_cep():
    """Fixture to create a sample CEP model"""
    return CEP(
        cep='01310100',
        logradouro='Avenida Paulista',
        complemento='',
        bairro='Bela Vista',
        localidade='São Paulo',
        uf='SP',
        ibge='3550308',
        gia='1004',
        ddd='11',
        siafi='7107',
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
    )


@pytest.fixture
def sample_ceps_list(sample_cep):
    """Fixture to create a list of sample CEPs"""
    cep2 = CEP(
        cep='01001900',
        logradouro='Praça da Sé',
        complemento='',
        bairro='Sé',
        localidade='São Paulo',
        uf='SP',
        ibge='3550308',
        gia='1004',
        ddd='11',
        siafi='7107',
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
    )
    return [sample_cep, cep2]


@pytest.fixture
def mock_database_manager():
    """Fixture to create a mock database manager"""
    mock = Mock()
    mock.connect.return_value = True
    mock.get_all_ceps.return_value = []
    return mock


class TestJSONExporter:
    """Test cases for JSONExporter class"""

    def test_init(self):
        """Test JSONExporter initialization"""
        exporter = JSONExporter()
        assert exporter.database_manager is None

    def test_init_with_database_manager(self, mock_database_manager):
        """Test JSONExporter initialization with database manager"""
        exporter = JSONExporter(database_manager=mock_database_manager)
        assert exporter.database_manager == mock_database_manager

    def test_export_ceps_list_to_file(self, sample_ceps_list, tmp_path):
        """Test exporting CEP list to JSON file"""
        exporter = JSONExporter()
        output_path = tmp_path / "test_export.json"
        
        result = exporter.export_ceps_list(
            ceps=sample_ceps_list,
            output_path=output_path,
            pretty=True,
            include_metadata=True
        )
        
        assert result is True
        assert output_path.exists()
        
        # Verify JSON content
        with open(output_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert 'metadata' in data
        assert 'ceps' in data
        assert len(data['ceps']) == 2
        assert data['ceps'][0]['cep'] == '01310100'
        assert data['ceps'][1]['cep'] == '01001900'

    def test_export_ceps_list_no_metadata(self, sample_ceps_list, tmp_path):
        """Test exporting CEP list without metadata"""
        exporter = JSONExporter()
        output_path = tmp_path / "test_export.json"
        
        result = exporter.export_ceps_list(
            ceps=sample_ceps_list,
            output_path=output_path,
            include_metadata=False
        )
        
        assert result is True
        
        with open(output_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Should be a list, not a dict with metadata
        assert isinstance(data, list)
        assert len(data) == 2

    def test_export_ceps_list_empty(self, tmp_path):
        """Test exporting empty CEP list"""
        exporter = JSONExporter()
        output_path = tmp_path / "test_export.json"
        
        result = exporter.export_ceps_list(
            ceps=[],
            output_path=output_path
        )
        
        assert result is False
        assert not output_path.exists()

    def test_export_to_string(self, sample_ceps_list):
        """Test exporting CEPs to JSON string"""
        exporter = JSONExporter()
        
        result = exporter.export_to_string(
            ceps=sample_ceps_list,
            pretty=True,
            include_metadata=True
        )
        
        assert result is not None
        data = json.loads(result)
        assert 'metadata' in data
        assert 'ceps' in data
        assert len(data['ceps']) == 2

    def test_export_to_string_empty(self):
        """Test exporting empty list to string"""
        exporter = JSONExporter()
        result = exporter.export_to_string(ceps=[])
        assert result is None

    @patch('src.exporters.json_exporter.DatabaseManager')
    def test_export_to_file_from_database(self, mock_db_class, sample_ceps_list, tmp_path):
        """Test exporting from database to file"""
        mock_db = Mock()
        mock_db.connect.return_value = True
        mock_db.get_all_ceps.return_value = sample_ceps_list
        mock_db_class.return_value = mock_db
        
        exporter = JSONExporter()
        output_path = tmp_path / "test_export.json"
        
        result = exporter.export_to_file(
            output_path=output_path,
            limit=10,
            offset=0
        )
        
        assert result is True
        assert output_path.exists()
        mock_db.connect.assert_called_once()
        mock_db.get_all_ceps.assert_called_once_with(limit=10, offset=0)

    @patch('src.exporters.json_exporter.DatabaseManager')
    def test_export_to_file_database_connection_failure(self, mock_db_class, tmp_path):
        """Test export when database connection fails"""
        mock_db = Mock()
        mock_db.connect.return_value = False
        mock_db_class.return_value = mock_db
        
        exporter = JSONExporter()
        output_path = tmp_path / "test_export.json"
        
        result = exporter.export_to_file(output_path=output_path)
        
        assert result is False
        assert not output_path.exists()


class TestXMLExporter:
    """Test cases for XMLExporter class"""

    def test_init(self):
        """Test XMLExporter initialization"""
        exporter = XMLExporter()
        assert exporter.database_manager is None

    def test_init_with_database_manager(self, mock_database_manager):
        """Test XMLExporter initialization with database manager"""
        exporter = XMLExporter(database_manager=mock_database_manager)
        assert exporter.database_manager == mock_database_manager

    def test_export_ceps_list_to_file(self, sample_ceps_list, tmp_path):
        """Test exporting CEP list to XML file"""
        exporter = XMLExporter()
        output_path = tmp_path / "test_export.xml"
        
        result = exporter.export_ceps_list(
            ceps=sample_ceps_list,
            output_path=output_path,
            pretty=True,
            include_metadata=True
        )
        
        assert result is True
        assert output_path.exists()
        
        # Verify XML content
        with open(output_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        root = fromstring(xml_content)
        assert root.tag == 'ceps'
        assert root.find('metadata') is not None
        assert root.find('ceps_list') is not None
        
        ceps_list = root.find('ceps_list')
        assert len(ceps_list.findall('cep')) == 2

    def test_export_ceps_list_no_metadata(self, sample_ceps_list, tmp_path):
        """Test exporting CEP list without metadata"""
        exporter = XMLExporter()
        output_path = tmp_path / "test_export.xml"
        
        result = exporter.export_ceps_list(
            ceps=sample_ceps_list,
            output_path=output_path,
            include_metadata=False
        )
        
        assert result is True
        
        with open(output_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        root = fromstring(xml_content)
        assert root.find('metadata') is None
        assert root.find('ceps_list') is not None

    def test_export_ceps_list_empty(self, tmp_path):
        """Test exporting empty CEP list"""
        exporter = XMLExporter()
        output_path = tmp_path / "test_export.xml"
        
        result = exporter.export_ceps_list(
            ceps=[],
            output_path=output_path
        )
        
        assert result is False
        assert not output_path.exists()

    def test_export_to_string(self, sample_ceps_list):
        """Test exporting CEPs to XML string"""
        exporter = XMLExporter()
        
        result = exporter.export_to_string(
            ceps=sample_ceps_list,
            pretty=True,
            include_metadata=True
        )
        
        assert result is not None
        root = fromstring(result)
        assert root.tag == 'ceps'
        assert root.find('ceps_list') is not None

    def test_export_to_string_empty(self):
        """Test exporting empty list to string"""
        exporter = XMLExporter()
        result = exporter.export_to_string(ceps=[])
        assert result is None

    def test_export_custom_root_element(self, sample_ceps_list, tmp_path):
        """Test exporting with custom root element name"""
        exporter = XMLExporter()
        output_path = tmp_path / "test_export.xml"
        
        result = exporter.export_ceps_list(
            ceps=sample_ceps_list,
            output_path=output_path,
            root_element_name='custom_root'
        )
        
        assert result is True
        
        with open(output_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        root = fromstring(xml_content)
        assert root.tag == 'custom_root'

    @patch('src.exporters.xml_exporter.DatabaseManager')
    def test_export_to_file_from_database(self, mock_db_class, sample_ceps_list, tmp_path):
        """Test exporting from database to file"""
        mock_db = Mock()
        mock_db.connect.return_value = True
        mock_db.get_all_ceps.return_value = sample_ceps_list
        mock_db_class.return_value = mock_db
        
        exporter = XMLExporter()
        output_path = tmp_path / "test_export.xml"
        
        result = exporter.export_to_file(
            output_path=output_path,
            limit=10,
            offset=0
        )
        
        assert result is True
        assert output_path.exists()
        mock_db.connect.assert_called_once()
        mock_db.get_all_ceps.assert_called_once_with(limit=10, offset=0)

    @patch('src.exporters.xml_exporter.DatabaseManager')
    def test_export_to_file_database_connection_failure(self, mock_db_class, tmp_path):
        """Test export when database connection fails"""
        mock_db = Mock()
        mock_db.connect.return_value = False
        mock_db_class.return_value = mock_db
        
        exporter = XMLExporter()
        output_path = tmp_path / "test_export.xml"
        
        result = exporter.export_to_file(output_path=output_path)
        
        assert result is False
        assert not output_path.exists()


