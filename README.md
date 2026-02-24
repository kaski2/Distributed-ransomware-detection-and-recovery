#### Setup your Python virtual environment
After cloning the repo you need to create a virtual environment:
```
python3 -m venv venv
```

Activate the freshly created venv with one of these two commands:
Unix:
```
source venv/bin/activate
```

Windows:
```
venv\Scripts\activate
```

Lastly install dependencies with this command:
```
pip install -r requirements.txt
```
Furthermore you need to add the directory you wish to monitor in the settings.ini file created automatically. Example settings.ini:
```
[settings]
monitored_dir_path = <Path to directory>
```

#### AWS S3 Bucket setup
This is used for sending and retrieving snapshots of tracked files.
1. Create an AWS account
2. Create a S3 Bucket
3. Configure IAM user with S3 permissions (AmazonS3FullAccess)
4. Save your credentials to .aws/credentials file. This is listed in .gitignore.

**Example of .aws/credentials file**
```
aws_access_key="your-IAM-access-key"
aws_secret_key="your-IAM-secret-key"
aws-server="aws-server-of-your-choice"
aws_bucket_name="your-custom-s3-bucket-name"
```
