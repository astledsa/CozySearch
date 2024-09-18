import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import json

def config_client(account_id, access_key, access_secret):

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=access_secret
    )

    config = Config(
        region_name='auto',
        s3={
            'addressing_style': 'virtual'
        }
    )

    return session.client('s3',
                          endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
                          config=config)

def upload_to_bucket(client, file, key, bucket_name):
   
    try:
        client.put_object(Bucket=bucket_name, Key=key, Body=file)
    except ClientError as e:
        print(f"Error in uploading object to bucket: {e}")

def download_paragraph_from_bucket(client, bucket_name, object_key):
   
    try:
        response = client.get_object(Bucket=bucket_name, Key=object_key)
        data = response['Body'].read()
        paragraph = json.loads(data.decode('utf-8'))
        return paragraph
    
    except ClientError as e:
        print(f"Error in downloading the object from bucket: {e}")
        return None

def list_bucket_items(client, bucket_name):
   
    try:
        response = client.list_objects_v2(Bucket=bucket_name)
        for obj in response.get('Contents', []):
            print(json.dumps(obj, indent=4, default=str))
    except ClientError as e:
        print(f"Error in getting the list of objects in bucket: {e}")
