"""
Error handler module for tracking and logging CEP processing errors
"""

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any
from enum import Enum

from src.utils.logger import setup_logger


class ErrorType(Enum):
    """Types of errors that can occur during CEP processing"""
    API_TIMEOUT = "api_timeout"
    API_ERROR = "api_error"
    CEP_NOT_FOUND = "cep_not_found"
    DATABASE_ERROR = "database_error"
    VALIDATION_ERROR = "validation_error"
    NETWORK_ERROR = "network_error"
    UNKNOWN_ERROR = "unknown_error"


class ErrorHandler:
    """
    Handler for tracking and logging CEP processing errors.
    Records errors to CSV file and provides detailed logging.
    """

    def __init__(self, errors_csv_path: Optional[Path] = None):
        """
        Initialize error handler.

        Args:
            errors_csv_path: Path to CSV file for storing errors.
                            Default: data/errors.csv
        """
        if errors_csv_path is None:
            errors_csv_path = Path("data/errors.csv")
        
        self.errors_csv_path = errors_csv_path
        self.logger = setup_logger(name="error_handler")
        
        # Ensure errors directory exists
        self.errors_csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize CSV file if it doesn't exist
        self._initialize_csv()

    def _initialize_csv(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not self.errors_csv_path.exists():
            try:
                with open(self.errors_csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['cep', 'error_type', 'error_message', 'timestamp', 'context'])
                self.logger.debug(f"Initialized errors CSV file: {self.errors_csv_path}")
            except Exception as e:
                self.logger.error(f"Failed to initialize errors CSV file: {e}")

    def record_error(
        self,
        cep: str,
        error_type: ErrorType,
        error_message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Record an error to CSV file and log it.

        Args:
            cep: CEP that caused the error
            error_type: Type of error (ErrorType enum)
            error_message: Error message description
            context: Optional context dictionary with additional information

        Returns:
            True if error was recorded successfully, False otherwise
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        context_str = str(context) if context else ""

        # Log error with appropriate level
        log_message = f"Error processing CEP {cep}: [{error_type.value}] {error_message}"
        if context:
            log_message += f" | Context: {context}"
        
        self.logger.error(log_message)

        # Write to CSV
        try:
            with open(self.errors_csv_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([cep, error_type.value, error_message, timestamp, context_str])
            
            self.logger.debug(f"Recorded error for CEP {cep} to {self.errors_csv_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to record error to CSV: {e}")
            return False

    def record_api_error(
        self,
        cep: str,
        error_message: str,
        status_code: Optional[int] = None,
        retry_attempt: Optional[int] = None
    ) -> bool:
        """
        Record an API-related error.

        Args:
            cep: CEP that caused the error
            error_message: Error message
            status_code: HTTP status code (if applicable)
            retry_attempt: Retry attempt number (if applicable)

        Returns:
            True if error was recorded successfully
        """
        context = {}
        if status_code:
            context['status_code'] = status_code
        if retry_attempt:
            context['retry_attempt'] = retry_attempt

        error_type = ErrorType.API_ERROR
        if "timeout" in error_message.lower():
            error_type = ErrorType.API_TIMEOUT
        elif "not found" in error_message.lower() or "404" in error_message:
            error_type = ErrorType.CEP_NOT_FOUND

        return self.record_error(cep, error_type, error_message, context if context else None)

    def record_database_error(
        self,
        cep: str,
        error_message: str,
        operation: Optional[str] = None
    ) -> bool:
        """
        Record a database-related error.

        Args:
            cep: CEP that caused the error
            error_message: Error message
            operation: Database operation that failed (e.g., 'save', 'get')

        Returns:
            True if error was recorded successfully
        """
        context = {'operation': operation} if operation else None
        return self.record_error(cep, ErrorType.DATABASE_ERROR, error_message, context)

    def record_validation_error(
        self,
        cep: str,
        error_message: str,
        validation_rule: Optional[str] = None
    ) -> bool:
        """
        Record a validation error.

        Args:
            cep: CEP that failed validation
            error_message: Error message
            validation_rule: Validation rule that failed

        Returns:
            True if error was recorded successfully
        """
        context = {'validation_rule': validation_rule} if validation_rule else None
        return self.record_error(cep, ErrorType.VALIDATION_ERROR, error_message, context)

    def get_errors(self, cep: Optional[str] = None, error_type: Optional[ErrorType] = None) -> List[Dict[str, Any]]:
        """
        Get errors from CSV file.

        Args:
            cep: Filter by CEP (None = all CEPs)
            error_type: Filter by error type (None = all types)

        Returns:
            List of error dictionaries
        """
        errors = []

        if not self.errors_csv_path.exists():
            return errors

        try:
            with open(self.errors_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Apply filters
                    if cep and row['cep'] != cep:
                        continue
                    if error_type and row['error_type'] != error_type.value:
                        continue
                    
                    errors.append({
                        'cep': row['cep'],
                        'error_type': row['error_type'],
                        'error_message': row['error_message'],
                        'timestamp': row['timestamp'],
                        'context': row['context']
                    })

        except Exception as e:
            self.logger.error(f"Error reading errors CSV file: {e}")

        return errors

    def get_error_count(self, cep: Optional[str] = None, error_type: Optional[ErrorType] = None) -> int:
        """
        Get count of errors.

        Args:
            cep: Filter by CEP (None = all CEPs)
            error_type: Filter by error type (None = all types)

        Returns:
            Number of errors matching the filters
        """
        return len(self.get_errors(cep=cep, error_type=error_type))

    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get summary of all errors.

        Returns:
            Dictionary with error statistics
        """
        errors = self.get_errors()
        
        summary = {
            'total_errors': len(errors),
            'by_type': {},
            'by_cep': {},
            'unique_ceps_with_errors': set()
        }

        for error in errors:
            # Count by type
            error_type = error['error_type']
            summary['by_type'][error_type] = summary['by_type'].get(error_type, 0) + 1

            # Count by CEP
            cep = error['cep']
            summary['by_cep'][cep] = summary['by_cep'].get(cep, 0) + 1
            summary['unique_ceps_with_errors'].add(cep)

        summary['unique_ceps_with_errors'] = len(summary['unique_ceps_with_errors'])
        
        return summary

    def clear_errors(self) -> bool:
        """
        Clear all errors from CSV file (reinitialize).

        Returns:
            True if cleared successfully
        """
        try:
            if self.errors_csv_path.exists():
                self.errors_csv_path.unlink()
            self._initialize_csv()
            self.logger.info("Cleared all errors from CSV file")
            return True
        except Exception as e:
            self.logger.error(f"Failed to clear errors: {e}")
            return False

