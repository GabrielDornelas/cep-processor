"""
AWS Lambda function for processing CEPs

This Lambda function can be triggered by:
- S3 events (when CSV is uploaded)
- EventBridge (scheduled)
- API Gateway (HTTP requests)
- SQS (message queue)

Usage:
    - Package this function with dependencies
    - Create Lambda function in AWS
    - Configure environment variables
    - Set up triggers as needed
"""

import json
import os
from typing import Dict, Any, Optional
import boto3

# Lambda layer should include the src package
from src.processors.viacep_client import ViaCEPClient
from src.storage.database import DatabaseManager
from src.processors.csv_handler import CSVHandler
from src.utils.logger import setup_logger

logger = setup_logger(name="lambda_function")
s3_client = boto3.client('s3')


def process_single_cep(cep: str, viacep_client: ViaCEPClient, db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    Process a single CEP.
    
    Args:
        cep: CEP string (8 digits)
        viacep_client: ViaCEP client instance
        db_manager: Database manager instance
        
    Returns:
        Dictionary with result or error
    """
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
            
            return {
                'success': True,
                'cep': cep,
                'data': result
            }
        else:
            return {
                'success': False,
                'cep': cep,
                'error': 'CEP not found'
            }
            
    except Exception as e:
        logger.error(f"Error processing CEP {cep}: {e}")
        return {
            'success': False,
            'cep': cep,
            'error': str(e)
        }


def process_s3_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process S3 event (CSV file uploaded).
    
    Args:
        event: Lambda event from S3
        
    Returns:
        Processing results
    """
    results = []
    
    # Initialize components
    viacep_client = ViaCEPClient(
        base_url=os.getenv('VIACEP_BASE_URL', 'https://viacep.com.br/ws'),
        timeout=int(os.getenv('REQUEST_TIMEOUT', '30')),
        retry_attempts=int(os.getenv('RETRY_ATTEMPTS', '3'))
    )
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")
    
    db_manager = DatabaseManager(database_url=database_url)
    
    if not db_manager.connect():
        raise Exception("Failed to connect to database")
    
    try:
        # Process each S3 record
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            logger.info(f"Processing S3 object: s3://{bucket}/{key}")
            
            # Download CSV from S3
            local_path = f"/tmp/{os.path.basename(key)}"
            s3_client.download_file(bucket, key, local_path)
            
            # Read and process CEPs
            csv_handler = CSVHandler()
            from pathlib import Path
            df = csv_handler.read_csv(Path(local_path))
            valid_ceps = csv_handler.validate_ceps(df)
            
            ceps_list = valid_ceps['cep'].tolist()
            logger.info(f"Found {len(ceps_list)} CEPs to process")
            
            # Process each CEP
            for cep in ceps_list:
                result = process_single_cep(cep, viacep_client, db_manager)
                results.append(result)
            
            # Cleanup
            os.remove(local_path)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed',
                'processed': len([r for r in results if r['success']]),
                'errors': len([r for r in results if not r['success']]),
                'results': results
            })
        }
        
    finally:
        db_manager.disconnect()


def process_api_gateway_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process API Gateway event (HTTP request).
    
    Args:
        event: Lambda event from API Gateway
        
    Returns:
        HTTP response
    """
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        cep = body.get('cep', '').replace('-', '').replace(' ', '')
        
        if not cep or len(cep) != 8:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid CEP format. Must be 8 digits.'})
            }
        
        # Initialize components
        viacep_client = ViaCEPClient(
            base_url=os.getenv('VIACEP_BASE_URL', 'https://viacep.com.br/ws'),
            timeout=int(os.getenv('REQUEST_TIMEOUT', '30')),
            retry_attempts=int(os.getenv('RETRY_ATTEMPTS', '3'))
        )
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Database configuration missing'})
            }
        
        db_manager = DatabaseManager(database_url=database_url)
        
        if not db_manager.connect():
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Database connection failed'})
            }
        
        try:
            result = process_single_cep(cep, viacep_client, db_manager)
            
            if result['success']:
                return {
                    'statusCode': 200,
                    'body': json.dumps(result['data'])
                }
            else:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': result.get('error', 'CEP not found')})
                }
        finally:
            db_manager.disconnect()
            
    except Exception as e:
        logger.error(f"Error processing API request: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def process_sqs_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process SQS event (message queue).
    
    Args:
        event: Lambda event from SQS
        
    Returns:
        Processing results
    """
    results = []
    
    # Initialize components
    viacep_client = ViaCEPClient(
        base_url=os.getenv('VIACEP_BASE_URL', 'https://viacep.com.br/ws'),
        timeout=int(os.getenv('REQUEST_TIMEOUT', '30')),
        retry_attempts=int(os.getenv('RETRY_ATTEMPTS', '3'))
    )
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")
    
    db_manager = DatabaseManager(database_url=database_url)
    
    if not db_manager.connect():
        raise Exception("Failed to connect to database")
    
    try:
        # Process each SQS record
        for record in event.get('Records', []):
            body = json.loads(record['body'])
            cep = body.get('cep', '').replace('-', '').replace(' ', '')
            
            if cep and len(cep) == 8:
                result = process_single_cep(cep, viacep_client, db_manager)
                results.append(result)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed',
                'processed': len([r for r in results if r['success']]),
                'errors': len([r for r in results if not r['success']]),
                'results': results
            })
        }
        
    finally:
        db_manager.disconnect()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler.
    
    Detects event source and routes to appropriate handler.
    
    Args:
        event: Lambda event
        context: Lambda context
        
    Returns:
        Response dictionary
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Detect event source
    if 'Records' in event:
        first_record = event['Records'][0]
        
        # S3 event
        if 's3' in first_record:
            return process_s3_event(event)
        
        # SQS event
        elif 'eventSource' in first_record and first_record['eventSource'] == 'aws:sqs':
            return process_sqs_event(event)
    
    # API Gateway event
    elif 'httpMethod' in event or 'requestContext' in event:
        return process_api_gateway_event(event)
    
    # EventBridge (scheduled) or direct invocation
    else:
        # For scheduled events, process a single CEP or batch
        # This is a simple example - adjust based on your needs
        logger.info("Processing scheduled/direct invocation")
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'DATABASE_URL not configured'})
            }
        
        # You can add custom logic here for scheduled processing
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Scheduled execution - add your logic here'})
        }
