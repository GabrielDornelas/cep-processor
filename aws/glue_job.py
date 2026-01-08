"""
AWS Glue Job for processing CEPs

This script runs on AWS Glue and processes CEPs from S3, 
queries ViaCEP API, and stores results in RDS PostgreSQL or S3.

Usage:
    - Upload this script to S3
    - Create Glue Job pointing to S3 location
    - Configure environment variables in Glue Job
    - Run job manually or schedule via EventBridge
"""

import sys
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

import boto3
from src.processors.viacep_client import ViaCEPClient
from src.storage.database import DatabaseManager
from src.processors.csv_handler import CSVHandler
from src.utils.logger import setup_logger

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_INPUT_PATH',
    'S3_OUTPUT_PATH',
    'DATABASE_URL',
    'RATE_LIMIT_PER_SECOND'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = setup_logger(name="glue_job")


def process_ceps_batch(ceps: List[str], viacep_client: ViaCEPClient, db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    Process a batch of CEPs.
    
    Args:
        ceps: List of CEP strings
        viacep_client: ViaCEP client instance
        db_manager: Database manager instance
        
    Returns:
        Dictionary with processing statistics
    """
    processed = 0
    errors = 0
    
    for cep in ceps:
        try:
            # Query ViaCEP
            result = viacep_client.query_cep(cep)
            
            if result:
                # Save to database
                from src.storage.models import CEP
                cep_model = CEP(
                    cep=result.get('cep', '').replace('-', ''),
                    logradouro=result.get('logradouro', ''),
                    complemento=result.get('complemento', ''),
                    bairro=result.get('bairro', ''),
                    cidade=result.get('localidade', ''),
                    uf=result.get('uf', ''),
                    ibge=result.get('ibge', ''),
                    ddd=result.get('ddd', '')
                )
                db_manager.save_cep(cep_model)
                processed += 1
            else:
                errors += 1
                
        except Exception as e:
            logger.error(f"Error processing CEP {cep}: {e}")
            errors += 1
    
    return {
        'processed': processed,
        'errors': errors,
        'total': len(ceps)
    }


def main():
    """Main Glue job execution."""
    logger.info("=" * 60)
    logger.info("AWS Glue Job - CEP Processor")
    logger.info("=" * 60)
    
    # Get configuration from job parameters
    s3_input_path = args.get('S3_INPUT_PATH')
    s3_output_path = args.get('S3_OUTPUT_PATH')
    database_url = args.get('DATABASE_URL')
    rate_limit = float(args.get('RATE_LIMIT_PER_SECOND', '2.0'))
    
    logger.info(f"S3 Input Path: {s3_input_path}")
    logger.info(f"S3 Output Path: {s3_output_path}")
    logger.info(f"Rate Limit: {rate_limit} req/s")
    
    # Initialize components
    viacep_client = ViaCEPClient(
        base_url=os.getenv('VIACEP_BASE_URL', 'https://viacep.com.br/ws'),
        timeout=int(os.getenv('REQUEST_TIMEOUT', '30')),
        retry_attempts=int(os.getenv('RETRY_ATTEMPTS', '3'))
    )
    
    db_manager = DatabaseManager(database_url=database_url)
    
    if not db_manager.connect():
        logger.error("Failed to connect to database")
        raise Exception("Database connection failed")
    
    logger.info("✓ Database connected")
    
    # Read CSV from S3
    logger.info(f"Reading CSV from S3: {s3_input_path}")
    df = spark.read.csv(s3_input_path, header=True, inferSchema=True)
    
    # Get CEPs from DataFrame
    csv_handler = CSVHandler()
    ceps_list = df.select("cep").rdd.flatMap(lambda x: x).collect()
    ceps_list = [str(cep).replace('-', '') for cep in ceps_list if cep]
    
    logger.info(f"Found {len(ceps_list)} CEPs to process")
    
    # Process in batches (to respect rate limits)
    batch_size = int(rate_limit * 60)  # Process 1 minute worth at a time
    total_processed = 0
    total_errors = 0
    
    for i in range(0, len(ceps_list), batch_size):
        batch = ceps_list[i:i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1}: {len(batch)} CEPs")
        
        stats = process_ceps_batch(batch, viacep_client, db_manager)
        total_processed += stats['processed']
        total_errors += stats['errors']
        
        logger.info(f"Batch complete: {stats['processed']} processed, {stats['errors']} errors")
    
    # Export results to S3 (optional)
    if s3_output_path:
        logger.info(f"Exporting results to S3: {s3_output_path}")
        all_ceps = db_manager.get_all_ceps()
        
        # Convert to DataFrame and write to S3
        from pyspark.sql import Row
        rows = [Row(
            cep=cep.cep,
            logradouro=cep.logradouro,
            complemento=cep.complemento,
            bairro=cep.bairro,
            cidade=cep.cidade,
            uf=cep.uf,
            ibge=cep.ibge,
            ddd=cep.ddd
        ) for cep in all_ceps]
        
        output_df = spark.createDataFrame(rows)
        output_df.write.mode('overwrite').parquet(s3_output_path)
        logger.info("✓ Results exported to S3")
    
    # Summary
    logger.info("=" * 60)
    logger.info("Job Summary")
    logger.info("=" * 60)
    logger.info(f"Total CEPs processed: {total_processed}")
    logger.info(f"Total errors: {total_errors}")
    logger.info(f"Total CEPs in database: {db_manager.count_ceps()}")
    logger.info("=" * 60)
    
    # Cleanup
    db_manager.disconnect()
    
    # Commit job
    job.commit()
    
    logger.info("✓ Glue job completed successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise
