"""
Lambda Function: check_nextflow_status
Called by Step Function to check if Nextflow pipeline completed
Checks SSM command status and optionally S3 output
"""

import boto3
import json
import os

ssm_client = boto3.client('ssm')
s3_client = boto3.client('s3')

EC2_INSTANCE_ID = os.environ.get('EC2_INSTANCE_ID', '')

def lambda_handler(event, context):
    print(f"Checking status for event: {json.dumps(event)}")
    
    # Get the SSM command ID from the previous step
    nextflow_result = event.get('nextflow_result', {})
    payload = nextflow_result.get('Payload', {})
    command_id = payload.get('command_id', '')
    run = event.get('run', '')
    
    if not command_id:
        print("No command_id found - cannot check SSM status")
        return {'status': 'RUNNING', 'reason': 'No command_id available yet'}
    
    # Check SSM command status
    try:
        response = ssm_client.get_command_invocation(
            CommandId=command_id,
            InstanceId=EC2_INSTANCE_ID
        )
        
        ssm_status = response['Status']
        print(f"SSM Command status: {ssm_status}")
        print(f"SSM Output: {response.get('StandardOutputContent', '')[:500]}")
        
        # Map SSM status to pipeline status
        if ssm_status == 'Success':
            # SSM command launched nextflow with -bg (background), 
            # so "Success" just means it was submitted. 
            # For a more robust check, look at S3 outputs.
            return check_s3_outputs(run)
        elif ssm_status in ['Failed', 'Cancelled', 'TimedOut']:
            return {
                'status': 'FAILED',
                'ssm_status': ssm_status,
                'error': response.get('StandardErrorContent', '')[:500]
            }
        else:
            # InProgress, Pending, Delayed
            return {'status': 'RUNNING', 'ssm_status': ssm_status}
    
    except ssm_client.exceptions.InvocationDoesNotExist:
        return {'status': 'RUNNING', 'reason': 'SSM invocation not found yet'}
    except Exception as e:
        print(f"Error checking SSM status: {e}")
        return {'status': 'RUNNING', 'reason': str(e)}


def check_s3_outputs(run):
    """
    Check if Nextflow has produced output files in S3.
    Adjust the output path to match your pipeline's actual outputs.
    """
    output_bucket = 'seqwell-users'
    output_prefix = f'yanyan/nextflow-work-dir/work/'
    
    try:
        # Check if there's a .nextflow.log or results in the work dir
        # This is a simple check - customize based on your pipeline outputs
        response = s3_client.list_objects_v2(
            Bucket=output_bucket,
            Prefix=output_prefix,
            MaxKeys=1
        )
        
        if response.get('KeyCount', 0) > 0:
            print(f"Found outputs in S3 for run {run}")
            return {
                'status': 'SUCCEEDED',
                'run': run,
                'output_location': f's3://{output_bucket}/{output_prefix}'
            }
        else:
            return {'status': 'RUNNING', 'reason': 'No S3 outputs yet'}
    
    except Exception as e:
        print(f"Error checking S3: {e}")
        # If we can't check S3, assume it's still running
        return {'status': 'RUNNING', 'reason': f'Could not check S3: {e}'}
