"""
XML exporter for CEP data
"""

from pathlib import Path
from typing import List, Optional
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from src.storage.models import CEP
from src.storage.database import DatabaseManager
from src.utils.logger import setup_logger


class XMLExporter:
    """
    Exporter for CEP data to XML format.
    """

    def __init__(self, database_manager: Optional[DatabaseManager] = None):
        """
        Initialize XML exporter.

        Args:
            database_manager: DatabaseManager instance (optional, will create if not provided)
        """
        self.logger = setup_logger(name="xml_exporter")
        self.database_manager = database_manager

    def export_to_file(
        self,
        output_path: Path,
        limit: Optional[int] = None,
        offset: int = 0,
        pretty: bool = True,
        include_metadata: bool = True,
        root_element_name: str = "ceps"
    ) -> bool:
        """
        Export CEPs from database to XML file.

        Args:
            output_path: Path to output XML file
            limit: Maximum number of CEPs to export (None = all)
            offset: Number of CEPs to skip
            pretty: If True, format XML with indentation
            include_metadata: If True, include export metadata in XML
            root_element_name: Name of root XML element

        Returns:
            True if export successful, False otherwise
        """
        try:
            if not self.database_manager:
                self.database_manager = DatabaseManager()
                if not self.database_manager.connect():
                    self.logger.error("Failed to connect to database")
                    return False

            self.logger.info(f"Exporting CEPs to XML: {output_path}")

            # Get CEPs from database
            ceps = self.database_manager.get_all_ceps(limit=limit, offset=offset)
            
            if not ceps:
                self.logger.warning("No CEPs found in database")
                return False

            # Create XML structure
            root = self._create_xml_structure(ceps, include_metadata, root_element_name, limit, offset)

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write XML file
            xml_string = self._element_to_string(root, pretty)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(xml_string)

            self.logger.info(f"Successfully exported {len(ceps)} CEPs to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error exporting to XML: {e}")
            return False

    def export_ceps_list(
        self,
        ceps: List[CEP],
        output_path: Path,
        pretty: bool = True,
        include_metadata: bool = True,
        root_element_name: str = "ceps"
    ) -> bool:
        """
        Export a list of CEP models to XML file.

        Args:
            ceps: List of CEP models to export
            output_path: Path to output XML file
            pretty: If True, format XML with indentation
            include_metadata: If True, include export metadata in XML
            root_element_name: Name of root XML element

        Returns:
            True if export successful, False otherwise
        """
        try:
            if not ceps:
                self.logger.warning("No CEPs provided for export")
                return False

            self.logger.info(f"Exporting {len(ceps)} CEPs to XML: {output_path}")

            # Create XML structure
            root = self._create_xml_structure(ceps, include_metadata, root_element_name)

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write XML file
            xml_string = self._element_to_string(root, pretty)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(xml_string)

            self.logger.info(f"Successfully exported {len(ceps)} CEPs to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error exporting to XML: {e}")
            return False

    def export_to_string(
        self,
        ceps: List[CEP],
        pretty: bool = True,
        include_metadata: bool = True,
        root_element_name: str = "ceps"
    ) -> Optional[str]:
        """
        Export CEPs to XML string.

        Args:
            ceps: List of CEP models to export
            pretty: If True, format XML with indentation
            include_metadata: If True, include export metadata in XML
            root_element_name: Name of root XML element

        Returns:
            XML string or None if error
        """
        try:
            if not ceps:
                return None

            # Create XML structure
            root = self._create_xml_structure(ceps, include_metadata, root_element_name)

            # Convert to string
            return self._element_to_string(root, pretty)

        except Exception as e:
            self.logger.error(f"Error exporting to XML string: {e}")
            return None

    def _create_xml_structure(
        self,
        ceps: List[CEP],
        include_metadata: bool,
        root_element_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> Element:
        """
        Create XML element structure from CEP list.

        Args:
            ceps: List of CEP models
            include_metadata: Whether to include metadata
            root_element_name: Name of root element
            limit: Limit used in query (for metadata)
            offset: Offset used in query (for metadata)

        Returns:
            Root XML element
        """
        root = Element(root_element_name)

        # Add metadata if requested
        if include_metadata:
            metadata = SubElement(root, 'metadata')
            SubElement(metadata, 'export_date').text = self._get_current_timestamp()
            SubElement(metadata, 'total_ceps').text = str(len(ceps))
            if limit is not None:
                SubElement(metadata, 'limit').text = str(limit)
            if offset > 0:
                SubElement(metadata, 'offset').text = str(offset)

        # Add CEPs
        ceps_element = SubElement(root, 'ceps_list')
        for cep in ceps:
            cep_dict = cep.to_dict()
            cep_elem = SubElement(ceps_element, 'cep')
            
            # Add all fields
            for key, value in cep_dict.items():
                if value is not None:
                    field_elem = SubElement(cep_elem, key)
                    field_elem.text = str(value)

        return root

    def _element_to_string(self, element: Element, pretty: bool = True) -> str:
        """
        Convert XML element to string.

        Args:
            element: XML element
            pretty: If True, format with indentation

        Returns:
            XML string
        """
        xml_string = tostring(element, encoding='unicode')
        
        if pretty:
            # Parse and pretty print
            dom = minidom.parseString(xml_string)
            return dom.toprettyxml(indent="  ", encoding=None)
        
        return xml_string

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
