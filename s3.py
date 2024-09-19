import os
import boto3
import json

def upload_to_s3(file_path, aws_config, bucket_name=None, s3_file_name=None, overwrite=False):
    """
    Uploads a file to an S3 bucket.

    :param file_path: Path of the file to upload
    :param aws_config: AWS configuration dictionary with keys for region, access key, secret key, and S3 folder name
    :param bucket_name: Name of the S3 bucket (if None, it uses the one from aws_config)
    :param s3_file_name: Name of the file in S3 (if None, use the same name as local file)
    :param overwrite: Whether to overwrite an existing file in S3
    """
    if not os.path.exists(file_path):
        print(f"Error: Local file '{file_path}' does not exist.")
        return
    
    if bucket_name is None:
        raise ValueError("No bucket name provided. Please specify a bucket name.")

    if s3_file_name is None:
        s3_file_name = aws_config['S3_folder_name'] + file_path.split('/')[-1]  # Use local file name if S3 name isn't provided

    s3_client = boto3.client('s3',
                             region_name=aws_config['AWS_REGION'],
                             aws_access_key_id=aws_config['AWS_ACCESS_KEY'],
                             aws_secret_access_key=aws_config['AWS_SECRET_KEY'])
    
    # Check if file exists in S3
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_file_name)
        if not overwrite:
            print(f"File '{s3_file_name}' already exists in S3 and overwrite is set to False.")
            return
    except:
        pass  # File doesn't exist, so proceed to upload

    try:
        s3_client.upload_file(file_path, bucket_name, s3_file_name)
        print(f"File '{file_path}' successfully uploaded to S3 as '{s3_file_name}'")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def download_from_s3(s3_file_name, download_path, aws_config, bucket_name=None):
    """
    Downloads a file from an S3 bucket.
    
    :param s3_file_name: Name of the file in S3
    :param download_path: Local path to save the downloaded file
    :param aws_config: AWS configuration dictionary with keys for region, access key, and secret key
    :param bucket_name: Name of the S3 bucket (if None, it uses the one from aws_config)
    """
    if bucket_name is None:
        bucket_name = aws_config['S3_BUCKET_NAME']

    if os.path.isdir(download_path):
        download_path = os.path.join(download_path, s3_file_name.split('/')[-1])

    s3_client = boto3.client('s3',
                             region_name=aws_config['AWS_REGION'],
                             aws_access_key_id=aws_config['AWS_ACCESS_KEY'],
                             aws_secret_access_key=aws_config['AWS_SECRET_KEY'])

    try:
        s3_client.download_file(bucket_name, s3_file_name, download_path)
        print(f"File '{s3_file_name}' successfully downloaded from S3 to '{download_path}'")
    except Exception as e:
        print(f"Error downloading from S3: {e}")

if __name__ == "__main__":
    # Example configuration passed to the functions
    with open('config/config.json') as config_file:
        aws_config = json.load(config_file)['aws']

    # Test by uploading a sample file with overwrite=False
    upload_to_s3('your-local-csv-file.csv', aws_config, overwrite=False)
