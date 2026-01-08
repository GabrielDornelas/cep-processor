"""
JSON exporter for CEP data
"""

import json
from pathlib import Path
from typing import List, Optional

from src.storage.models import CEP
from src.storage.database import DatabaseManager
from src.utils.logger import setup_logger


class JSONExporter:
    """
    Exporter for CEP data to JSON format.
    """

    def __init__(self, database_manager: Optional[DatabaseManager] = None):
        """
        Initialize JSON exporter.

        Args:
            database_manager: DatabaseManager instance (optional, will create if not provided)
        """
        self.logger = setup_logger(name="json_exporter")
        self.database_manager = database_manager

    def export_to_file(
        self,
        output_path: Path,
        limit: Optional[int] = None,
        offset: int = 0,
        pretty: bool = True,
        include_metadata: bool = True
    ) -> bool:
        """
        Export CEPs from database to JSON file.

        Args:
            output_path: Path to output JSON file
            limit: Maximum number of CEPs to export (None = all)
            offset: Number of CEPs to skip
            pretty: If True, format JSON with indentation
            include_metadata: If True, include export metadata in JSON

        Returns:
            True if export successful, False otherwise
        """
        try:
            if not self.database_manager:
                self.database_manager = DatabaseManager()
                if not self.database_manager.connect():
                    self.logger.error("Failed to connect to database")
                    return False

            self.logger.info(f"Exporting CEPs to JSON: {output_path}")

            # Get CEPs from database
            ceps = self.database_manager.get_all_ceps(limit=limit, offset=offset)
            
            if not ceps:
                self.logger.warning("No CEPs found in database")
                return False

            # Convert to dictionaries
            ceps_data = [cep.to_dict() for cep in ceps]

            # Prepare export data
            if include_metadata:
                export_data = {
                    'metadata': {
                        'export_date': self._get_current_timestamp(),
                        'total_ceps': len(ceps_data),
                        'limit': limit,
                        'offset': offset
                    },
                    'ceps': ceps_data
                }
            else:
                export_data = ceps_data

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump(export_data, f, ensure_ascii=False)

            self.logger.info(f"Successfully exported {len(ceps_data)} CEPs to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error exporting to JSON: {e}")
            return False

    def export_ceps_list(
        self,
        ceps: List[CEP],
        output_path: Path,
        pretty: bool = True,
        include_metadata: bool = True
    ) -> bool:
        """
        Export a list of CEP models to JSON file.

        Args:
            ceps: List of CEP models to export
            output_path: Path to output JSON file
            pretty: If True, format JSON with indentation
            include_metadata: If True, include export metadata in JSON

        Returns:
            True if export successful, False otherwise
        """
        try:
            if not ceps:
                self.logger.warning("No CEPs provided for export")
                return False

            self.logger.info(f"Exporting {len(ceps)} CEPs to JSON: {output_path}")

            # Convert to dictionaries
            ceps_data = [cep.to_dict() for cep in ceps]

            # Prepare export data
            if include_metadata:
                export_data = {
                    'metadata': {
                        'export_date': self._get_current_timestamp(),
                        'total_ceps': len(ceps_data)
                    },
                    'ceps': ceps_data
                }
            else:
                export_data = ceps_data

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump(export_data, f, ensure_ascii=False)

            self.logger.info(f"Successfully exported {len(ceps_data)} CEPs to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error exporting to JSON: {e}")
            return False

    def export_to_string(
        self,
        ceps: List[CEP],
        pretty: bool = True,
        include_metadata: bool = True
    ) -> Optional[str]:
        """
        Export CEPs to JSON string.

        Args:
            ceps: List of CEP models to export
            pretty: If True, format JSON with indentation
            include_metadata: If True, include export metadata in JSON

        Returns:
            JSON string or None if error
        """
        try:
            if not ceps:
                return None

            # Convert to dictionaries
            ceps_data = [cep.to_dict() for cep in ceps]

            # Prepare export data
            if include_metadata:
                export_data = {
                    'metadata': {
                        'export_date': self._get_current_timestamp(),
                        'total_ceps': len(ceps_data)
                    },
                    'ceps': ceps_data
                }
            else:
                export_data = ceps_data

            # Convert to JSON string
            if pretty:
                return json.dumps(export_data, indent=2, ensure_ascii=False)
            else:
                return json.dumps(export_data, ensure_ascii=False)

        except Exception as e:
            self.logger.error(f"Error exporting to JSON string: {e}")
            return None

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
