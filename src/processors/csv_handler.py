"""
CSV handler module for reading and validating CEP data using Pandas
"""

from pathlib import Path
from typing import List

import pandas as pd

from src.utils.logger import setup_logger


class CSVHandler:
    """
    Handler for reading and validating CEP CSV files using Pandas.
    Validates that CEPs are 8-digit numbers (valid Brazilian CEP format).
    """

    def __init__(self):
        """Initialize the CSV handler."""
        self.logger = setup_logger(name="csv_handler")

    def _is_valid_cep(self, cep: str) -> bool:
        """
        Validate CEP format.
        Brazilian CEPs must be exactly 8 digits.

        Args:
            cep: CEP string to validate (can be with or without hyphen)

        Returns:
            True if valid Brazilian CEP, False otherwise
        """
        if not cep or not isinstance(cep, str):
            return False

        # Remove any whitespace and hyphens
        cep_clean = str(cep).strip().replace('-', '').replace(' ', '')

        # Must be exactly 8 digits
        if not cep_clean.isdigit() or len(cep_clean) != 8:
            return False

        return True

    def read_csv(self, csv_path: Path) -> pd.DataFrame:
        """
        Read CEP CSV file and return as DataFrame.

        Args:
            csv_path: Path to CSV file

        Returns:
            DataFrame with CEP data

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV file is empty or invalid
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        self.logger.info(f"Reading CSV file: {csv_path}")

        try:
            df = pd.read_csv(csv_path, dtype={'cep': str})
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {e}")
            raise ValueError(f"Failed to read CSV file: {e}")

        if df.empty:
            raise ValueError("CSV file is empty")

        # Check if 'cep' column exists
        if 'cep' not in df.columns:
            raise ValueError("CSV file must contain 'cep' column")

        self.logger.info(f"Loaded {len(df)} CEPs from CSV file")
        return df

    def validate_ceps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate CEPs in DataFrame and add validation status.

        Args:
            df: DataFrame with CEP data (must have 'cep' column)

        Returns:
            DataFrame with added 'is_valid' column
        """
        if 'cep' not in df.columns:
            raise ValueError("DataFrame must contain 'cep' column")

        self.logger.info(f"Validating {len(df)} CEPs...")

        # Apply validation function
        df['is_valid'] = df['cep'].apply(self._is_valid_cep)

        valid_count = df['is_valid'].sum()
        invalid_count = len(df) - valid_count

        self.logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid CEPs")

        if invalid_count > 0:
            invalid_ceps = df[~df['is_valid']]['cep'].tolist()
            self.logger.warning(f"Found {invalid_count} invalid CEPs. Examples: {invalid_ceps[:5]}")

        return df

    def get_valid_ceps(self, df: pd.DataFrame) -> List[str]:
        """
        Get list of valid CEPs from DataFrame.

        Args:
            df: DataFrame with validated CEPs (must have 'is_valid' column)

        Returns:
            List of valid CEP strings (8 digits, no hyphen)
        """
        if 'is_valid' not in df.columns:
            df = self.validate_ceps(df)

        valid_df = df[df['is_valid']].copy()

        # Ensure CEPs are formatted as 8 digits (no hyphen)
        valid_ceps = valid_df['cep'].apply(
            lambda x: str(x).strip().replace('-', '').replace(' ', '')
        ).tolist()

        self.logger.info(f"Extracted {len(valid_ceps)} valid CEPs")
        return valid_ceps

    def get_invalid_ceps(self, df: pd.DataFrame) -> List[str]:
        """
        Get list of invalid CEPs from DataFrame.

        Args:
            df: DataFrame with validated CEPs (must have 'is_valid' column)

        Returns:
            List of invalid CEP strings
        """
        if 'is_valid' not in df.columns:
            df = self.validate_ceps(df)

        invalid_df = df[~df['is_valid']].copy()
        invalid_ceps = invalid_df['cep'].apply(
            lambda x: str(x).strip().replace('-', '').replace(' ', '')
        ).tolist()

        return invalid_ceps

    def load_and_validate(self, csv_path: Path) -> pd.DataFrame:
        """
        Load CSV file and validate all CEPs in one operation.

        Args:
            csv_path: Path to CSV file

        Returns:
            DataFrame with validated CEPs (includes 'is_valid' column)

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV file is empty or invalid
        """
        df = self.read_csv(csv_path)
        df = self.validate_ceps(df)
        return df

    def get_cep_count(self, df: pd.DataFrame) -> dict:
        """
        Get statistics about CEPs in DataFrame.

        Args:
            df: DataFrame with CEP data

        Returns:
            Dictionary with statistics (total, valid, invalid)
        """
        if 'is_valid' not in df.columns:
            df = self.validate_ceps(df)

        stats = {
            'total': len(df),
            'valid': df['is_valid'].sum(),
            'invalid': (~df['is_valid']).sum()
        }

        return stats

