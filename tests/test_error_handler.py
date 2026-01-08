"""
Unit tests for error handler module
"""

from pathlib import Path
import csv
import tempfile
import os

from src.utils.error_handler import ErrorHandler, ErrorType


class TestErrorHandler:
    """Test cases for ErrorHandler class"""

    def test_init(self):
        """Test ErrorHandler initialization"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            assert handler.errors_csv_path == errors_path
            assert errors_path.exists()

    def test_init_creates_directory(self):
        """Test that initialization creates directory if it doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "subdir" / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            assert errors_path.parent.exists()
            assert errors_path.exists()

    def test_initialize_csv(self):
        """Test CSV file initialization"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            # Check that CSV file has correct headers
            with open(errors_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                assert headers == ['cep', 'error_type', 'error_message', 'timestamp', 'context']

    def test_record_error(self):
        """Test recording an error"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            result = handler.record_error(
                cep='01310100',
                error_type=ErrorType.API_ERROR,
                error_message='Connection timeout',
                context={'retry': 3}
            )
            
            assert result is True
            
            # Verify error was written to CSV
            errors = handler.get_errors()
            assert len(errors) == 1
            assert errors[0]['cep'] == '01310100'
            assert errors[0]['error_type'] == 'api_error'
            assert errors[0]['error_message'] == 'Connection timeout'

    def test_record_api_error(self):
        """Test recording API error"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            result = handler.record_api_error(
                cep='01310100',
                error_message='Request timeout',
                status_code=408,
                retry_attempt=2
            )
            
            assert result is True
            
            errors = handler.get_errors()
            assert len(errors) == 1
            assert errors[0]['error_type'] == 'api_timeout'
            assert 'status_code' in errors[0]['context']

    def test_record_api_error_cep_not_found(self):
        """Test recording CEP not found error"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            handler.record_api_error(
                cep='00000000',
                error_message='CEP not found (404)'
            )
            
            errors = handler.get_errors()
            assert errors[0]['error_type'] == 'cep_not_found'

    def test_record_database_error(self):
        """Test recording database error"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            result = handler.record_database_error(
                cep='01310100',
                error_message='Connection failed',
                operation='save'
            )
            
            assert result is True
            
            errors = handler.get_errors()
            assert errors[0]['error_type'] == 'database_error'
            assert 'operation' in errors[0]['context']

    def test_record_validation_error(self):
        """Test recording validation error"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            result = handler.record_validation_error(
                cep='123',
                error_message='Invalid CEP format',
                validation_rule='8_digits'
            )
            
            assert result is True
            
            errors = handler.get_errors()
            assert errors[0]['error_type'] == 'validation_error'
            assert 'validation_rule' in errors[0]['context']

    def test_get_errors_all(self):
        """Test getting all errors"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            # Record multiple errors
            handler.record_error('01310100', ErrorType.API_ERROR, 'Error 1')
            handler.record_error('01310101', ErrorType.DATABASE_ERROR, 'Error 2')
            handler.record_error('01310102', ErrorType.API_ERROR, 'Error 3')
            
            errors = handler.get_errors()
            assert len(errors) == 3

    def test_get_errors_filter_by_cep(self):
        """Test filtering errors by CEP"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            handler.record_error('01310100', ErrorType.API_ERROR, 'Error 1')
            handler.record_error('01310101', ErrorType.API_ERROR, 'Error 2')
            handler.record_error('01310100', ErrorType.DATABASE_ERROR, 'Error 3')
            
            errors = handler.get_errors(cep='01310100')
            assert len(errors) == 2
            assert all(e['cep'] == '01310100' for e in errors)

    def test_get_errors_filter_by_type(self):
        """Test filtering errors by type"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            handler.record_error('01310100', ErrorType.API_ERROR, 'Error 1')
            handler.record_error('01310101', ErrorType.DATABASE_ERROR, 'Error 2')
            handler.record_error('01310102', ErrorType.API_ERROR, 'Error 3')
            
            errors = handler.get_errors(error_type=ErrorType.API_ERROR)
            assert len(errors) == 2
            assert all(e['error_type'] == 'api_error' for e in errors)

    def test_get_error_count(self):
        """Test getting error count"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            handler.record_error('01310100', ErrorType.API_ERROR, 'Error 1')
            handler.record_error('01310101', ErrorType.API_ERROR, 'Error 2')
            handler.record_error('01310102', ErrorType.DATABASE_ERROR, 'Error 3')
            
            assert handler.get_error_count() == 3
            assert handler.get_error_count(cep='01310100') == 1
            assert handler.get_error_count(error_type=ErrorType.API_ERROR) == 2

    def test_get_error_summary(self):
        """Test getting error summary"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            handler.record_error('01310100', ErrorType.API_ERROR, 'Error 1')
            handler.record_error('01310101', ErrorType.API_ERROR, 'Error 2')
            handler.record_error('01310102', ErrorType.DATABASE_ERROR, 'Error 3')
            handler.record_error('01310100', ErrorType.VALIDATION_ERROR, 'Error 4')
            
            summary = handler.get_error_summary()
            
            assert summary['total_errors'] == 4
            assert summary['by_type']['api_error'] == 2
            assert summary['by_type']['database_error'] == 1
            assert summary['by_type']['validation_error'] == 1
            assert summary['by_cep']['01310100'] == 2
            assert summary['unique_ceps_with_errors'] == 3

    def test_clear_errors(self):
        """Test clearing all errors"""
        with tempfile.TemporaryDirectory() as tmpdir:
            errors_path = Path(tmpdir) / "errors.csv"
            handler = ErrorHandler(errors_csv_path=errors_path)
            
            handler.record_error('01310100', ErrorType.API_ERROR, 'Error 1')
            handler.record_error('01310101', ErrorType.API_ERROR, 'Error 2')
            
            assert handler.get_error_count() == 2
            
            result = handler.clear_errors()
            assert result is True
            assert handler.get_error_count() == 0
            
            # File should still exist but only have headers
            assert errors_path.exists()

