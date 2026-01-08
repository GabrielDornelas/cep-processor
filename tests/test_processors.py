"""
Unit tests for processors module
"""

import pytest
import pandas as pd
from pathlib import Path
import csv

from src.processors.csv_handler import CSVHandler


class TestCSVHandler:
    """Test cases for CSVHandler class"""

    def test_init(self):
        """Test CSVHandler initialization"""
        handler = CSVHandler()
        assert handler is not None
        assert handler.logger is not None

    def test_is_valid_cep(self):
        """Test CEP validation"""
        handler = CSVHandler()

        # Valid CEPs (8 digits - any Brazilian CEP)
        assert handler._is_valid_cep("01001900") is True  # São Paulo
        assert handler._is_valid_cep("01310-100") is True  # With hyphen
        assert handler._is_valid_cep(" 01001900 ") is True  # With spaces
        assert handler._is_valid_cep("20040020") is True  # Rio de Janeiro
        assert handler._is_valid_cep("30130100") is True  # Belo Horizonte
        assert handler._is_valid_cep("40020100") is True  # Salvador
        assert handler._is_valid_cep("10001220") is True  # Valid CEP (not SP)

        # Invalid CEPs
        assert handler._is_valid_cep("0100190") is False  # Wrong length (7 digits)
        assert handler._is_valid_cep("010019001") is False  # Wrong length (9 digits)
        assert handler._is_valid_cep("abc12345") is False  # Non-numeric
        assert handler._is_valid_cep("") is False  # Empty
        assert handler._is_valid_cep(None) is False  # None

    def test_read_csv(self, tmp_path):
        """Test CSV file reading"""
        handler = CSVHandler()

        # Create test CSV file
        csv_file = tmp_path / "test_ceps.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['cep'])
            writer.writerow(['01001900'])
            writer.writerow(['01001901'])
            writer.writerow(['01001902'])

        df = handler.read_csv(csv_file)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'cep' in df.columns
        assert df['cep'].iloc[0] == '01001900'

    def test_read_csv_file_not_found(self):
        """Test reading non-existent CSV file"""
        handler = CSVHandler()
        fake_path = Path("nonexistent_file.csv")

        with pytest.raises(FileNotFoundError):
            handler.read_csv(fake_path)

    def test_read_csv_empty_file(self, tmp_path):
        """Test reading empty CSV file"""
        handler = CSVHandler()

        csv_file = tmp_path / "empty.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['cep'])

        with pytest.raises(ValueError, match="CSV file is empty"):
            handler.read_csv(csv_file)

    def test_read_csv_missing_cep_column(self, tmp_path):
        """Test reading CSV without 'cep' column"""
        handler = CSVHandler()

        csv_file = tmp_path / "invalid.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['code', 'name'])
            writer.writerow(['01001900', 'Test'])

        with pytest.raises(ValueError, match="must contain 'cep' column"):
            handler.read_csv(csv_file)

    def test_validate_ceps(self):
        """Test CEP validation in DataFrame"""
        handler = CSVHandler()

        df = pd.DataFrame({
            'cep': ['01001900', '01001901', '20040020', '0100190', '01310-100']
        })

        df_validated = handler.validate_ceps(df)

        assert 'is_valid' in df_validated.columns
        assert df_validated['is_valid'].iloc[0] == True  # 01001900 (SP)
        assert df_validated['is_valid'].iloc[1] == True  # 01001901 (SP)
        assert df_validated['is_valid'].iloc[2] == True  # 20040020 (RJ - valid)
        assert df_validated['is_valid'].iloc[3] == False  # 0100190 (wrong length)
        assert df_validated['is_valid'].iloc[4] == True  # 01310-100 (with hyphen)

    def test_get_valid_ceps(self):
        """Test getting valid CEPs list"""
        handler = CSVHandler()

        df = pd.DataFrame({
            'cep': ['01001900', '01001901', '20040020', '01310-100']
        })
        df = handler.validate_ceps(df)

        valid_ceps = handler.get_valid_ceps(df)

        assert len(valid_ceps) == 4
        assert '01001900' in valid_ceps
        assert '01001901' in valid_ceps
        assert '20040020' in valid_ceps  # RJ CEP - now valid
        assert '01310100' in valid_ceps  # Hyphen removed

    def test_get_invalid_ceps(self):
        """Test getting invalid CEPs list"""
        handler = CSVHandler()

        df = pd.DataFrame({
            'cep': ['01001900', '20040020', '0100190', 'abc12345']
        })
        df = handler.validate_ceps(df)

        invalid_ceps = handler.get_invalid_ceps(df)

        assert len(invalid_ceps) == 2
        assert '0100190' in invalid_ceps  # Wrong length
        assert 'abc12345' in invalid_ceps  # Non-numeric
        assert '01001900' not in invalid_ceps
        assert '20040020' not in invalid_ceps  # Valid RJ CEP

    def test_load_and_validate(self, tmp_path):
        """Test loading and validating CSV in one operation"""
        handler = CSVHandler()

        # Create test CSV file
        csv_file = tmp_path / "test_ceps.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['cep'])
            writer.writerow(['01001900'])  # SP
            writer.writerow(['20040020'])  # RJ - valid
            writer.writerow(['0100190'])   # Invalid (wrong length)

        df = handler.load_and_validate(csv_file)

        assert isinstance(df, pd.DataFrame)
        assert 'is_valid' in df.columns
        assert len(df) == 3
        assert df['is_valid'].sum() == 2  # 2 valid CEPs

    def test_get_cep_count(self):
        """Test getting CEP statistics"""
        handler = CSVHandler()

        df = pd.DataFrame({
            'cep': ['01001900', '20040020', '30130100', '0100190']
        })
        df = handler.validate_ceps(df)

        stats = handler.get_cep_count(df)

        assert stats['total'] == 4
        assert stats['valid'] == 3  # 3 valid CEPs (SP, RJ, MG)
        assert stats['invalid'] == 1  # 1 invalid (wrong length)

