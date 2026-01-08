"""
Main orchestrator for CEP Processor

Orchestrates the complete workflow:
1. Web scraping to collect CEPs
2. Publishing CEPs to queue
3. Processing CEPs from queue via ViaCEP API
4. Storing results in PostgreSQL
5. Exporting data (optional)
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

from src.collectors.web_scraper import WebScraper
from src.processors.csv_handler import CSVHandler
from src.processors.viacep_client import ViaCEPClient
from src.queue.queue_manager import QueueManager
from src.storage.database import DatabaseManager
from src.exporters.json_exporter import JSONExporter
from src.exporters.xml_exporter import XMLExporter
from src.utils.logger import setup_logger


class CEPProcessor:
    """
    Main orchestrator for CEP processing workflow.
    """

    def __init__(self):
        """Initialize the CEP processor."""
        self.logger = setup_logger(name="cep_processor")
        
        # Initialize components
        self.db_manager: Optional[DatabaseManager] = None
        self.queue_manager: Optional[QueueManager] = None
        self.viacep_client: Optional[ViaCEPClient] = None
        self.csv_handler = CSVHandler()

    def setup_connections(self) -> bool:
        """
        Setup database and queue connections.

        Returns:
            True if all connections successful, False otherwise
        """
        self.logger.info("Setting up connections...")
        
        # Database connection
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            self.logger.error("Failed to connect to database")
            return False
        self.logger.info("✓ Database connected")
        
        # Create tables if needed
        if not self.db_manager.create_tables():
            self.logger.error("Failed to create database tables")
            return False
        
        # Queue connection
        self.queue_manager = QueueManager()
        if not self.queue_manager.connect():
            self.logger.error("Failed to connect to RabbitMQ")
            return False
        self.logger.info("✓ RabbitMQ connected")
        
        # ViaCEP client
        self.viacep_client = ViaCEPClient()
        self.logger.info("✓ ViaCEP client initialized")
        
        return True

    def collect_ceps(self, max_ceps: Optional[int] = None, output_path: Optional[Path] = None) -> Optional[Path]:
        """
        Collect CEPs via web scraping.

        Args:
            max_ceps: Maximum number of CEPs to collect (uses MAX_CEPS env var or 10000 if None)
            output_path: Path to save CSV file (uses CEPS_CSV_PATH env var or default if None)

        Returns:
            Path to CSV file or None if failed
        """
        self.logger.info("=" * 60)
        self.logger.info("Step 1: Collecting CEPs via web scraping")
        self.logger.info("=" * 60)
        
        # Use config defaults if not provided
        # Get CSV path from environment or use default
        if not output_path:
            csv_path_str = os.getenv('CEPS_CSV_PATH', 'data/ceps_collected.csv')
            output_path = Path(csv_path_str)
            if not output_path.is_absolute():
                project_root = Path(__file__).parent.parent
                output_path = project_root / csv_path_str
        
        max_ceps = max_ceps or int(os.getenv('MAX_CEPS', '10000'))
        
        scraper = WebScraper(
            base_url=os.getenv('SCRAPING_BASE_URL', 'https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/'),
            delay=float(os.getenv('SCRAPING_DELAY', '2.0')),
            max_ceps=max_ceps
        )
        
        try:
            csv_path = scraper.scrape(output_path=output_path)
            self.logger.info(f"✓ Collected CEPs saved to: {csv_path}")
            return csv_path
        except Exception as e:
            self.logger.error(f"Failed to collect CEPs: {e}")
            return None

    def publish_ceps_to_queue(self, csv_path: Path) -> bool:
        """
        Read CEPs from CSV and publish to queue.

        Args:
            csv_path: Path to CSV file with CEPs

        Returns:
            True if successful, False otherwise
        """
        self.logger.info("=" * 60)
        self.logger.info("Step 2: Publishing CEPs to queue")
        self.logger.info("=" * 60)
        
        try:
            # Read and validate CEPs
            df = self.csv_handler.read_csv(csv_path)
            df = self.csv_handler.validate_ceps(df)
            valid_ceps = self.csv_handler.get_valid_ceps(df)
            
            if not valid_ceps:
                self.logger.error("No valid CEPs found in CSV")
                return False
            
            self.logger.info(f"Found {len(valid_ceps)} valid CEPs")
            
            # Publish to queue
            if not self.queue_manager:
                self.logger.error("Queue manager not initialized")
                return False
            
            published_count = self.queue_manager.publish_multiple_ceps(valid_ceps)
            if published_count != len(valid_ceps):
                self.logger.warning(f"Only {published_count} of {len(valid_ceps)} CEPs were published successfully")
                return False
            
            self.logger.info(f"✓ Published {published_count} CEPs to queue")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish CEPs to queue: {e}")
            return False

    def process_queue(self, limit: Optional[int] = None) -> int:
        """
        Process CEPs from queue, query ViaCEP API, and store in database.

        Args:
            limit: Maximum number of CEPs to process (None = all)

        Returns:
            Number of CEPs successfully processed
        """
        self.logger.info("=" * 60)
        self.logger.info("Step 3: Processing CEPs from queue")
        self.logger.info("=" * 60)
        
        if not all([self.queue_manager, self.viacep_client, self.db_manager]):
            self.logger.error("Required components not initialized")
            return 0
        
        processed_count = 0
        
        def process_cep(cep: str) -> bool:
            """Callback function to process a single CEP."""
            nonlocal processed_count
            
            try:
                # Query ViaCEP API
                viacep_data = self.viacep_client.query_cep(cep)
                
                if not viacep_data:
                    self.logger.warning(f"CEP {cep} not found or error occurred")
                    return False
                
                # Save to database
                self.db_manager.save_cep(viacep_data)
                processed_count += 1
                
                return True
                
            except Exception as e:
                self.logger.error(f"Error processing CEP {cep}: {e}")
                return False
        
        try:
            self.queue_manager.consume_ceps(process_cep, stop_after=limit)
            self.logger.info(f"✓ Processed {processed_count} CEPs")
            return processed_count
            
        except Exception as e:
            self.logger.error(f"Error processing queue: {e}")
            return processed_count

    def export_data(self, json_path: Optional[Path] = None, xml_path: Optional[Path] = None) -> bool:
        """
        Export CEPs from database to JSON and/or XML.

        Args:
            json_path: Path for JSON export (None = skip)
            xml_path: Path for XML export (None = skip)

        Returns:
            True if at least one export successful, False otherwise
        """
        if not json_path and not xml_path:
            return False
        
        self.logger.info("=" * 60)
        self.logger.info("Step 4: Exporting data")
        self.logger.info("=" * 60)
        
        success = False
        
        if json_path:
            json_exporter = JSONExporter(database_manager=self.db_manager)
            if json_exporter.export_to_file(json_path, pretty=True, include_metadata=True):
                self.logger.info(f"✓ JSON exported to: {json_path}")
                success = True
            else:
                self.logger.error(f"Failed to export JSON to: {json_path}")
        
        if xml_path:
            xml_exporter = XMLExporter(database_manager=self.db_manager)
            if xml_exporter.export_to_file(xml_path, pretty=True, include_metadata=True):
                self.logger.info(f"✓ XML exported to: {xml_path}")
                success = True
            else:
                self.logger.error(f"Failed to export XML to: {xml_path}")
        
        return success

    def run_full_workflow(
        self,
        collect: bool = True,
        max_ceps: Optional[int] = None,
        process_limit: Optional[int] = None,
        export_json: bool = True,
        export_xml: bool = True,
        csv_path: Optional[Path] = None
    ) -> bool:
        """
        Run the complete workflow.

        Args:
            collect: Whether to collect CEPs via web scraping
            max_ceps: Maximum CEPs to collect (uses MAX_CEPS env var or 10000 if None)
            process_limit: Maximum CEPs to process from queue (None = all)
            export_json: Whether to export JSON
            export_xml: Whether to export XML
            csv_path: Path to CSV file (if None, uses environment variable or default)

        Returns:
            True if workflow completed successfully, False otherwise
        """
        self.logger.info("=" * 60)
        self.logger.info("CEP Processor - Full Workflow")
        self.logger.info("=" * 60)
        
        # Setup connections
        if not self.setup_connections():
            return False
        
        # Get CSV path from argument, environment, or use default
        if csv_path is None:
            csv_path_str = os.getenv('CEPS_CSV_PATH', 'data/ceps_collected.csv')
            csv_path = Path(csv_path_str)
            if not csv_path.is_absolute():
                project_root = Path(__file__).parent.parent
                csv_path = project_root / csv_path_str
        
        # Step 1: Collect CEPs (if requested)
        if collect:
            csv_path = self.collect_ceps(max_ceps=max_ceps)
            if not csv_path:
                self.logger.error("Failed to collect CEPs")
                return False
        elif not csv_path.exists():
            # Verify existing CSV exists when skipping collection
            self.logger.error(f"CSV file not found: {csv_path}")
            return False
        
        # Step 2: Publish to queue
        if not self.publish_ceps_to_queue(csv_path):
            return False
        
        # Step 3: Process queue
        processed = self.process_queue(limit=process_limit)
        if processed == 0:
            self.logger.warning("No CEPs were processed")
        
        # Step 4: Export data
        if export_json or export_xml:
            data_dir = Path("data")
            json_path = data_dir / "ceps_export.json" if export_json else None
            xml_path = data_dir / "ceps_export.xml" if export_xml else None
            self.export_data(json_path=json_path, xml_path=xml_path)
        
        # Summary
        total_in_db = self.db_manager.count_ceps()
        self.logger.info("=" * 60)
        self.logger.info("Workflow Summary")
        self.logger.info("=" * 60)
        self.logger.info(f"CEPs processed: {processed}")
        self.logger.info(f"Total CEPs in database: {total_in_db}")
        self.logger.info("=" * 60)
        
        return True

    def cleanup(self):
        """Cleanup connections."""
        if self.queue_manager:
            self.queue_manager.disconnect()
        if self.db_manager:
            self.db_manager.disconnect()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="CEP Processor - Collect, process, and export Brazilian postal codes"
    )
    
    parser.add_argument(
        '--skip-collect',
        action='store_true',
        help='Skip web scraping, use existing CSV file'
    )
    
    parser.add_argument(
        '--max-ceps',
        type=int,
        help='Maximum number of CEPs to collect (default: from MAX_CEPS env var or 10000)'
    )
    
    parser.add_argument(
        '--process-limit',
        type=int,
        help='Maximum number of CEPs to process from queue (default: all)'
    )
    
    parser.add_argument(
        '--no-export',
        action='store_true',
        help='Skip data export'
    )
    
    parser.add_argument(
        '--export-format',
        choices=['json', 'xml', 'both'],
        default='both',
        help='Export format (default: both)'
    )
    
    parser.add_argument(
        '--csv-path',
        type=Path,
        help='Path to CSV file with CEPs (if skipping collection)'
    )
    
    args = parser.parse_args()
    
    processor = CEPProcessor()
    
    try:
        success = processor.run_full_workflow(
            collect=not args.skip_collect,
            max_ceps=args.max_ceps,
            process_limit=args.process_limit,
            export_json=args.export_format in ['json', 'both'] and not args.no_export,
            export_xml=args.export_format in ['xml', 'both'] and not args.no_export,
            csv_path=args.csv_path
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        processor.logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        processor.logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        processor.cleanup()


if __name__ == "__main__":
    main()
