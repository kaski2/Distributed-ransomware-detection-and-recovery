

if __name__ == "__main__":
    import configparser
    import os

    config = configparser.ConfigParser()
    config.read_string('[DEFAULT]\n' + open('.aws/variables').read())

    for key, value in config['DEFAULT'].items():
        os.environ[key.upper()] = value.strip('"').strip("'")

    from aws_client import upload_snapshot_to_s3

    # Test upload
    snapshot = {"key": "test_snapshot",
                "data": "halooooo"}
    success = upload_snapshot_to_s3(snapshot)
    if success:
        print("Snapshot uploaded successfully.")
    else:
        print("Failed to upload snapshot.")
    
    # Test retrieve
    print("\n--- Testing Retrieval ---")
    from aws_client import get_snapshot_from_s3
    get_snapshot_from_s3()
    

