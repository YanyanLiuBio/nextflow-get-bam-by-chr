"""
Lambda Function: run_nextflow
Called by Step Function - sends SSM Run Command to your EC2 instance to run Nextflow
"""

import boto3
import json
import os
import time

ssm_client = boto3.client('ssm')
ec2_client = boto3.client('ec2')

# The EC2 instance ID where Nextflow runs - set as environment variable
EC2_INSTANCE_ID = os.environ.get('EC2_INSTANCE_ID', '')

def lambda_handler(event, context):
    print(f"Running Nextflow with params: {json.dumps(event)}")
    
    run = event['run']
    analysis = event['analysis']
    dev = event['dev']
    chr_val = event['chr']
    
    # Build the nextflow command - same as your bash script
    nextflow_command = f"""
#!/bin/bash
set -e

# Load environment
source /home/ec2-user/.bashrc || true
export PATH="/home/ec2-user/miniconda/bin:$PATH"

echo "Starting Nextflow pipeline at $(date)"
echo "Run: {run}"
echo "Analysis: {analysis}"  
echo "Chr: {chr_val}"
echo "Dev: {dev}"

cd /home/ec2-user

nextflow run YanyanLiuBio/nextflow-get-bam-by-chr \\
  -work-dir s3://seqwell-users/yanyan/nextflow-work-dir/work/ \\
  --run {run} \\
  --analysis {analysis} \\
  --chr {chr_val} \\
  --dev {dev} \\
  -bg -resume

echo "Nextflow submitted at $(date)"
echo "NEXTFLOW_LAUNCHED=true"
"""

    # Send command to EC2 via SSM
    response = ssm_client.send_command(
        InstanceIds=[EC2_INSTANCE_ID],
        DocumentName='AWS-RunShellScript',
        Parameters={
            'commands': [nextflow_command],
            'executionTimeout': ['3600']  # 1 hour timeout for the SSM command itself
        },
        Comment=f'Nextflow pipeline: {run}',
        CloudWatchOutputConfig={
            'CloudWatchLogGroupName': f'/nextflow-automation/ssm-commands',
            'CloudWatchOutputEnabled': True
        }
    )
    
    command_id = response['Command']['CommandId']
    print(f"SSM Command submitted. CommandId: {command_id}")
    
    # Wait briefly to confirm command started
    time.sleep(5)
    
    # Check initial status
    status_response = ssm_client.get_command_invocation(
        CommandId=command_id,
        InstanceId=EC2_INSTANCE_ID
    )
    
    print(f"Initial SSM status: {status_response['Status']}")
    
    return {
        'statusCode': 200,
        'command_id': command_id,
        'instance_id': EC2_INSTANCE_ID,
        'run': run,
        'ssm_status': status_response['Status'],
        'message': f'Nextflow submitted via SSM. CommandId: {command_id}'
    }
