#!/usr/bin/env python3
"""
Simple script to export CEPs from database to JSON and XML formats.
No processing required - just exports existing data.
"""

from pathlib import Path
from src.storage.database import DatabaseManager
from src.exporters.json_exporter import JSONExporter
from src.exporters.xml_exporter import XMLExporter

def main():
    """Export CEPs from database to JSON and XML."""
    db = DatabaseManager()
    
    if not db.connect():
        print("❌ Failed to connect to database")
        return
    
    print("✓ Connected to database")
    
    # Export to JSON
    json_path = Path('data/ceps_export.json')
    if JSONExporter(db).export_to_file(json_path):
        print(f"✓ JSON exported to: {json_path}")
    else:
        print(f"❌ Failed to export JSON")
    
    # Export to XML
    xml_path = Path('data/ceps_export.xml')
    if XMLExporter(db).export_to_file(xml_path):
        print(f"✓ XML exported to: {xml_path}")
    else:
        print(f"❌ Failed to export XML")
    
    db.disconnect()
    print("✓ Export completed")

if __name__ == "__main__":
    main()
