""" Python script to demonstrate some of the functionalities from the low level API
for AWS SDK S3 service using Functional Programming.

Such functionalities includes: creation of a S3 bucket, uploading and downloading files, listing and
deleting objects from a bucket, deleting a bucket, getting its ACL, as well as getting and setting
the CORS and policies from a bucket.
"""

#%%
import logging
import sys
import time
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import yfinance as yf
import os
import json


#########################################################
# Creation
#########################################################

def create_bucket(client, bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        return True
    except ClientError as e:
        logging.error(e)
        return False

#########################################################
#  Listing
#########################################################

def get_list_of_existing_buckets(client):
    """Retrieve the list of existing buckets
    
    :param client: S3 Client used to connect with AWS
    :return: List of existing buckets
    """
    try:
        response = client.list_buckets()
        return [bucket["Name"] for bucket in response['Buckets']]
    except ClientError as e:
        logging.error(e)
        return []


def check_if_bucket_exists(client, bucket_name):
    """Check if a bucket exists
    
    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to check
    :return: True if bucket exists, else False
    """

    buckets = get_list_of_existing_buckets(client)
    return True if bucket_name in buckets else False


def get_list_objects_in_bucket(client, bucket_name):
    """Retrieve the list of objects in a bucket
    
    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to retrieve objects from
    :return: List of objects in the bucket
    """

    response = client.list_objects_v2(Bucket=bucket_name)
    file_count = response['KeyCount']
    if file_count > 0:
        list_of_objects = [content['Key'] for content in response["Contents"]]
        return list_of_objects
    else:
        logging.info(f"Bucket '{bucket_name}' is empty.")
        return []
    
    

#########################################################
# Upload
#########################################################

def upload_file(client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param client: S3 Client used to connect with AWS
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = client.upload_file(file_name, bucket, object_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False

#########################################################
# Download
#########################################################

def download_objects_from_bucket(client, bucket_name, object_list, directory_destiny):
    """Download objects from a bucket

    :param client: S3 Client used to connect with AWS
    :param bucket_name: string
    :param object_list: list of strings
    :param directory_destiny: string
    :return: True if all objects were downloaded, else False
    """

    try:
        os.makedirs(os.path.dirname(directory_destiny), exist_ok=True)
        file_name_list = [f"{directory_destiny}/{os.path.basename(object_name)}" for object_name in object_list]

        for object_name, file_name in zip(object_list, file_name_list):
            response = client.download_file(bucket_name, object_name, file_name)
            logging.info(f"Downloaded {object_name} to {file_name}")
        return True
    except ClientError as e:
        logging.error(e)
        return False

#########################################################
# Deletion
#########################################################

def delete_objects_from_bucket(client, bucket_name, object_list):
    """Delete objects from a bucket

    :param client: S3 Client used to connect with AWS
    :param bucket_name: string
    :param object_list: list of strings
    :return: True if all objects were deleted, else False
    """
    if not isinstance(object_list, list):
        raise TypeError("object_list must be a list")
    if len(object_list) == 0:
        raise ValueError("object_list must not be empty")

    try:
        if len(object_list) == 1:
            response = client.delete_object(Bucket=bucket_name, Key=object_list[0])
        elif len(object_list) > 1:
            Objects = [{'Key': k} for k in object_list]

            response = client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': Objects,
                    'Quiet': False
                }
            )
        return True
    except Exception as e:
        logging.error(e)
        return False


def nuke_bucket(client, bucket_name):
    """Delete all objects in a bucket and then the bucket itself
    
    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to delete
    :return: True if bucket and all objects deleted, else False
    """

    try:
        objects = get_list_objects_in_bucket(client, bucket_name)
        delete_objects_from_bucket(client, bucket_name, objects)
        delete_bucket(client, bucket_name)
        logging.info(f"Bucket '{bucket_name}' and all it's {len(objects)} objects were deleted.")
        return True
    except ClientError as e:
        logging.error(e)
        return False


def delete_bucket(client, bucket_name):
    """Delete a bucket

    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to delete
    :return: True if bucket deleted, else False
    """

    try:
        response = client.delete_bucket(Bucket=bucket_name)
        return True
    
    except Exception as e:
        logging.error(e)
        return False
    
#########################################################
# Policies
#########################################################

def get_bucket_policy(client, bucket_name):
    """Retrieve the policy of the specified bucket
    
    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to retrieve policy from
    :return: Bucket policy
    """

    try:
        response = client.get_bucket_policy(Bucket=bucket_name)
        return response['Policy']
    except Exception as e:
        logging.error(e)
        return None


def create_bucket_policy(client, bucket_name, policy_document):
    """Set a bucket policy
    
    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to set policy for
    :param policy_document: Policy document
    :return: True if policy was set, else False
    """

    try:
        response = client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=policy_document
        )
        return True
    except Exception as e:
        logging.error(e)
        return False


def delete_bucket_policy(client, bucket_name):
    """Delete the policy of the specified bucket
    
    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to delete policy from
    :return: True if policy was deleted, else False
    """

    try:
        response = client.delete_bucket_policy(Bucket=bucket_name)
        return True
    except Exception as e:
        logging.error(e)
        return False

#########################################################
# Access Control List - ACL
#########################################################

# Get a bucket access control list
def get_bucket_acl(client, bucket_name):
    """Retrieve the access control list of the specified bucket
    
    :param client: S3 Client used to connect with AWS
    :param bucket_name: Bucket to retrieve ACL from
    :return: Bucket ACL
    """

    try:
        response = client.get_bucket_acl(Bucket=bucket_name)
        return response['Grants']
    except Exception as e:
        logging.error(e)
        return None

#########################################################
# Cross-Origin Resource Sharing - CORS
#########################################################

def get_bucket_cors(client, bucket_name):
    """Retrieve the CORS configuration rules of an Amazon S3 bucket

    :param client: S3 Client used to connect with AWS
    :param bucket_name: string
    :return: List of the bucket's CORS configuration rules. If no CORS
    configuration exists, return empty list. If error, return None.
    """
    
    try:
        response = client.get_bucket_cors(Bucket=bucket_name)
        return response['CORSRules']
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchCORSConfiguration':
            return []
        else:
            logging.error(e)
            return None


def set_bucket_cors(client, bucket_name, cors_configuration):
    """Set the CORS configuration rules of an Amazon S3 bucket

    :param client: S3 Client used to connect with AWS
    :param bucket_name: string
    :param cors_configuration: dict, the CORS configuration rules
    :return: The response returned by the put_bucket_cors method.
    """

    try:
        response = client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration=cors_configuration
        )
        return response
    except ClientError as e:
        logging.error(e)
        return None


def delete_bucket_cors(client, bucket_name):
    """Delete the CORS configuration rules of an Amazon S3 bucket

    :param client: S3 Client used to connect with AWS
    :param bucket_name: string
    :return: The response returned by the delete_bucket_cors method.
    """

    try:
        response = client.delete_bucket_cors(Bucket=bucket_name)
        return response
    except ClientError as e:
        logging.error(e)
        return None

#########################################################
# Auxiliary Functions
#########################################################

def on_premise_ingestion(ticker_list, directory):
    """Ingest data on-premise from yfinance data sources

    :param ticker_list: list of strings
    :param directory: string
    """

    os.makedirs(os.path.dirname(directory), exist_ok=True)
    financial_data = []
    for ticker in ticker_list:
        data = yf.Ticker(ticker).history(period="max", interval="1d")
        financial_data.append(data)
        data.to_csv(f"./{directory}/{ticker}.csv")
        print(f"Ingested data from ticker '{ticker}'")
        time.sleep(3)


def get_absolute_file_paths_and_names(directory):
    """Get absolute file paths and names of files in a directory
    
    :param directory: string
    :return: list of tuples, each tuple contains the absolute file path and the file name"""

    path = os.path.abspath(directory)
    absolute_file_paths = [entry.path for entry in os.scandir(path) if entry.is_file()]
    file_names = [entry.name for entry in os.scandir(path) if entry.is_file()]
    return absolute_file_paths, file_names

def print_line(message):
    line = '='*60
    print(f"\n{line}\n{message.upper()}")

#%%

#########################################################
# Demonstration
#########################################################

def usage_demo():
    print_line("Beginning of Amazon S3 bucket functionalities demonstration")

    # Redirects the logging output to a file AND to the console
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
            # format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s",
        datefmt='%m/%d/%Y %H:%M:%S',
        handlers=[
            logging.FileHandler("logging.log"),
            logging.StreamHandler(sys.stdout)
        ])

    # Global variables
    load_dotenv('/home/user/.env')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = "dms-nasdaq"
    tickers = ["AMZN", "AAPL", "TSLA", "GOOG", "NFLX"]
    upload_directory = "./files_to_upload/"
    download_directory = "./downloaded_files/"
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Ingest data on premise
    print_line("Ingesting data on premise")

    on_premise_ingestion(ticker_list=tickers, directory=upload_directory)
    abs_file_paths, file_names = get_absolute_file_paths_and_names(upload_directory)

    # Create bucket and verify that it is created
    print_line("Bucket creation + verification")
    bucket_list = get_list_of_existing_buckets(s3_client)
    print(f"List of buckets (BEFORE): {bucket_list}")
    print(f"Creating bucket named {bucket_name}...")
    create_bucket(s3_client, bucket_name)
    bucket_list = get_list_of_existing_buckets(s3_client)
    print(f"List of buckets (AFTER): {bucket_list}")

    # Upload files and verify that they are in the bucket
    print_line("Local data transfer to bucket + verification")
    object_list = get_list_objects_in_bucket(s3_client, bucket_name=bucket_name)
    print(f"List of objects in bucket (BEFORE): {object_list}")
    print("Uploading files to bucket...")
    for file in abs_file_paths:
        upload_file(s3_client, file, bucket_name)
    object_list = get_list_objects_in_bucket(s3_client, bucket_name=bucket_name)
    print(f"List of objects in bucket (AFTER): {object_list}")

    # Delete some objects and verify that it is deleted from the bucket
    print_line("Object deletion")
    print(f"Deleting object {object_list[0:2]} from bucket...")
    delete_objects_from_bucket(s3_client, bucket_name, object_list[0:2])
    object_list = get_list_objects_in_bucket(s3_client, bucket_name=bucket_name)
    print(f"Objects in bucket after single deletion: {object_list}")

    # Download files from bucket
    print_line("Download objects from bucket to local folder")
    download_objects_from_bucket(s3_client, bucket_name, object_list, download_directory)

    # Get Access Control List (ACL) of a bucket
    print_line("Get Access Control List (ACL) of a bucket")
    acl = get_bucket_acl(s3_client, bucket_name)
    print(f"Bucket {bucket_name} has ACL grants: {acl}.")

    # Test CORS configuration
    print_line("Testing CORS configuration")
    bucket_cors_rules = get_bucket_cors(s3_client, bucket_name)
    print(f"Bucket {bucket_name} has CORS rules (BEFORE ADDITION): {json.dumps(bucket_cors_rules)}.")

    # Define the configuration rules
    cors_configuration = {
        'CORSRules': [{
            'AllowedHeaders': ['Authorization'],
            'AllowedMethods': ['GET', 'PUT'],
            'AllowedOrigins': ['*'],
            'ExposeHeaders': ['GET', 'PUT'],
            'MaxAgeSeconds': 3000
        }]
    }

    set_bucket_cors(s3_client, bucket_name, cors_configuration)
    bucket_cors_rules = get_bucket_cors(s3_client, bucket_name)
    print(f"Bucket {bucket_name} has CORS rules (AFTER ADDITION): {json.dumps(bucket_cors_rules)}.")

    delete_bucket_cors(s3_client, bucket_name)
    bucket_cors_rules = get_bucket_cors(s3_client, bucket_name)
    print(f"Bucket {bucket_name} has CORS rules (AFTER DELETION): {json.dumps(bucket_cors_rules)}.")


    # Test bucket policy configuration
    print_line("Testing bucket policiy configuration")
    policy = get_bucket_policy(s3_client, bucket_name)
    print(f"Bucket {bucket_name} has policy (BEFORE ADDITION): {json.dumps(policy)}.")

    # Create a bucket policy
    bucket_policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'AddPerm',
            'Effect': 'Allow',
            'Principal': '*',
            'Action': ['s3:GetObject'],
            'Resource': f'arn:aws:s3:::{bucket_name}/*'
        }]
    }

    # Convert the policy from JSON dict to string
    bucket_policy = json.dumps(bucket_policy)

    create_bucket_policy(s3_client, bucket_name, bucket_policy)
    policy = get_bucket_policy(s3_client, bucket_name)
    print(f"Bucket {bucket_name} has policy (AFTER ADDITION): {json.dumps(policy)}.")

    delete_bucket_policy(s3_client, bucket_name)
    policy = get_bucket_policy(s3_client, bucket_name)
    print(f"Bucket {bucket_name} has policy (AFTER DELETION): {json.dumps(policy)}.")

    # Delete bucket and all it's contents
    print_line("Delete bucket and all it's contents")
    nuke_bucket(s3_client, bucket_name)

    print_line("End of Amazon S3 bucket functionalities demonstration")

#%%

if __name__ == '__main__':
    usage_demo()
