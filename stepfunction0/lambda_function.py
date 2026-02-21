"""
Lambda Function: trigger_nextflow_stepfunction
Triggered by: S3 event when a .csv file is uploaded to s3://seqwell-stepfunction/
Purpose: Parse the CSV and start the Step Function execution
"""

import boto3
import csv
import json
import os
import urllib.parse
from datetime import datetime

s3_client = boto3.client('s3')
sfn_client = boto3.client('stepfunctions')

# Set this to your Step Function ARN after deployment
STEP_FUNCTION_ARN = os.environ.get('STEP_FUNCTION_ARN', '')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    # --- 1. Get the S3 file info from the event ---
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    print(f"File uploaded: s3://{bucket}/{key}")
    
    # Only process .csv files
    if not key.endswith('.csv'):
        print(f"Skipping non-CSV file: {key}")
        return {'statusCode': 200, 'body': 'Not a CSV file, skipping'}
    
    # --- 2. Read the CSV file from S3 ---
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8').strip()
    print(f"CSV content: {content}")
    
    # Parse CSV - format: run,analysis,dev,chr
    # Example: 20251224_MiSeqi100-Morty,human,false,chr22
    lines = content.splitlines()
    
    # Skip header line if present
    data_line = lines[0]
    if data_line.lower().startswith('run') or ',' not in data_line:
        if len(lines) > 1:
            data_line = lines[1]
        else:
            raise ValueError(f"CSV file appears to be header-only or empty: {content}")
    
    parts = [p.strip() for p in data_line.split(',')]
    if len(parts) < 4:
        raise ValueError(f"CSV must have 4 columns (run,analysis,dev,chr), got: {data_line}")
    
    run, analysis, dev, chr_val = parts[0], parts[1], parts[2], parts[3]
    
    # Derive the run name from the CSV filename (e.g., 20251224_MiSeqi100-Morty.csv -> 20251224_MiSeqi100-Morty)
    csv_filename = key.split('/')[-1].replace('.csv', '')
    
    print(f"Parsed parameters - run: {run}, analysis: {analysis}, dev: {dev}, chr: {chr_val}")
    
    # --- 3. Build the Step Function input ---
    execution_input = {
        "run": run,
        "analysis": analysis,
        "dev": dev,
        "chr": chr_val,
        "source_bucket": bucket,
        "source_key": key,
        "triggered_at": datetime.utcnow().isoformat() + "Z"
    }
    
    # --- 4. Start the Step Function execution ---
    execution_name = f"{csv_filename}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    # Step Function execution names can only have letters, numbers, hyphens, underscores
    execution_name = execution_name.replace('_', '-')[:80]
    
    response = sfn_client.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=execution_name,
        input=json.dumps(execution_input)
    )
    
    print(f"Started Step Function execution: {response['executionArn']}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Step Function started successfully',
            'executionArn': response['executionArn'],
            'parameters': execution_input
        })
    }
