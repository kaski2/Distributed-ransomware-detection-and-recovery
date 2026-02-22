def _get_env_variables():
    import configparser
    import os

    config = configparser.ConfigParser()
    config.read_string('[DEFAULT]\n' + open('.aws/variables').read())

    for key, value in config['DEFAULT'].items():
        os.environ[key.upper()] = value.strip('"').strip("'")

def _initialize_s3_client():
    _get_env_variables()
    import boto3
    import os
    s3 = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
        region_name=os.environ['AWS_SERVER']
    )
    return s3

def get_snapshot_from_s3():
    import os
    try:
        s3 = _initialize_s3_client()
        bucket = os.environ['AWS_BUCKET_NAME']
        # TODO:
        # make this dynamic to get the latest snapshot or a specific snapshot based on timestamp or key
        key = "snapshots/test_snapshot.json"

        print(f"Retrieving from bucket: {bucket}, key: {key}")
        response = s3.get_object(Bucket=bucket, Key=key)

        retrieved_data = response['Body'].read().decode('utf-8')
        print(f"Retrieved data: {retrieved_data}")
        print(f"Content-Type: {response.get('ContentType')}")
        print(f"Last Modified: {response.get('LastModified')}")
        
    except Exception as e:
        print(f"[AWS S3] Failed to retrieve snapshot: {e}")

def upload_snapshot_to_s3(snapshot: dict) -> bool:
    s3 = _initialize_s3_client()
    import os
    try:
        bucket = os.environ['AWS_BUCKET_NAME']
        key = snapshot.get('key')
        data = snapshot.get('data')
        
        # TODO:
        # add timestamp to file name
        s3.put_object(Bucket=bucket, Key=f"snapshots/{key}.json", Body=data, ContentType="application/json")
        return True
    except Exception as e:
        print(f"[AWS S3] Failed to upload snapshot: {e}")
        return False
