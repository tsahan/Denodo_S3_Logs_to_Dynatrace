import boto3
import requests
import json
import base64
import re

def lambda_handler(event, context):
    # Initialize Dynatrace API endpoint and API token
    dynatrace_endpoint = ""
    dynatrace_api_token = ""
    
    level_timestamp_pattern = r'\w+\s+\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}'

    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Get the S3 object from the event
    s3_object = event['Records'][0]['s3']

    # Get the S3 bucket name and key
    s3_bucket = s3_object['bucket']['name']
    s3_key = s3_object['object']['key']

    # Download the S3 object
    s3_object = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    s3_data = s3_object['Body'].read()
    
    # Read lines
    lines = s3_data.decode('utf-8').splitlines()
    lines = [line.strip() for line in lines if line.strip()]
    
    # Add values into a list of dictionaries
    log_entries = []
    idx = 0
   
    level_next, timestamp_next = re.search(level_timestamp_pattern, lines[0]).group().replace('  ',' ').split(' ')
    
    # Find log entries
    while idx < len(lines):
        content = lines[idx]
        idx += 1
        level, timestamp = level_next, timestamp_next
        
        # Add additional lines of the log entry if exist
        while idx < len(lines):
            result_object = re.search(level_timestamp_pattern, lines[idx])
            if result_object:
                level_next, timestamp_next = result_object.group().replace('  ',' ').split(' ')
                break
            
            content += '\n' + lines[idx]
            idx += 1

        log_entries.append({'content': content, 'level': level, 'timestamp': timestamp, 'log.source': 'S3'})
    
    # Convert the data to JSON format
    json_output = json.dumps(log_entries)

    # Send the log entries to Dynatrace
    try:
        headers = {'Authorization': 'Api-Token ' + dynatrace_api_token, 'Content-Type': 'application/json; charset=utf-8'}
        response = requests.post(dynatrace_endpoint, headers=headers, data=json_output)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_error:
        print (f'Http error occurred: Http error or a log event is older than 24 hours: {http_error}')
    except requests.exceptions.ConnectionError as conn_error:
        print (f'Connection error occurred: {conn_error}')
    except requests.exceptions.Timeout as timeout_error:
        print (f'Timeout error occurred: {timeout_error}')
    except requests.exceptions.RequestException as request_exception:
        print(f'Request exception occurred: {request_exception}')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
    else:
        return 'Logs pushed to Dynatrace successfully.'