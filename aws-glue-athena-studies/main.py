# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#%%
"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with AWS Glue to
create and manage crawlers, databases, and jobs.
"""

import logging
import os
import pprint
import sys
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from iam_role_wrapper import attach_policy, create_role, list_policies, list_roles
from s3_utils import check_if_bucket_exists, create_bucket, upload_data_to_bucket, nuke_bucket
import boto3
from glue_wrapper import GlueWrapper
import json

from boto3.exceptions import S3UploadFailedError

logger = logging.getLogger(__name__)


# ##########################################################
# Glue Crawler
# ##########################################################


def get_crawler(client, name):
    """
    Gets information about a crawler.

    :param name: The name of the crawler to look up.
    :return: Data about the crawler.
    """
    crawler = None
    try:
        response = client.get_crawler(Name=name)
        crawler = response['Crawler']
    except ClientError as err:
        if err.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info("Crawler %s doesn't exist.", name)
        else:
            logger.error(
                "Couldn't get crawler %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    return crawler


def create_crawler(client, name, role_arn, db_name, db_prefix, s3_target):
    """
    Creates a crawler that can crawl the specified target and populate a
    database in your AWS Glue Data Catalog with metadata that describes the data
    in the target.

    :param name: The name of the crawler.
    :param role_arn: The Amazon Resource Name (ARN) of an AWS Identity and Access
                        Management (IAM) role that grants permission to let AWS Glue
                        access the resources it needs.
    :param db_name: The name to give the database that is created by the crawler.
    :param db_prefix: The prefix to give any database tables that are created by
                        the crawler.
    :param s3_target: The URL to an S3 bucket that contains data that is
                        the target of the crawler.
    """
    try:
        client.create_crawler(
            Name=name,
            Role=role_arn,
            DatabaseName=db_name,
            TablePrefix=db_prefix,
            Targets={'S3Targets': [{'Path': s3_target}]})
    except ClientError as err:
        logger.error(
            "Couldn't create crawler. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise


def start_crawler(client, name):
    """
    Starts a crawler. The crawler crawls its configured target and creates
    metadata that describes the data it finds in the target data source.

    :param name: The name of the crawler to start.
    """
    try:
        client.start_crawler(Name=name)
    except ClientError as err:
        logger.error(
            "Couldn't start crawler %s. Here's why: %s: %s", name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise


def delete_crawler(client, name):
    """
    Deletes a crawler.

    :param name: The name of the crawler to delete.
    """
    try:
        client.delete_crawler(Name=name)
    except ClientError as err:
        logger.error(
            "Couldn't delete crawler %s. Here's why: %s: %s", name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise

# ##########################################################
# Table and Database
# ##########################################################

def get_tables(glue_client, db_name):
    """
    Gets a list of tables in a Data Catalog database.

    :param db_name: The name of the database to query.
    :return: The list of tables in the database.
    """
    try:
        response = glue_client.get_tables(DatabaseName=db_name)
    except ClientError as err:
        logger.error(
            "Couldn't get tables %s. Here's why: %s: %s", db_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['TableList']


def get_database(glue_client, name):
    """
    Gets information about a database in your Data Catalog.

    :param name: The name of the database to look up.
    :return: Information about the database.
    """
    try:
        response = glue_client.get_database(Name=name)
    except ClientError as err:
        logger.error(
            "Couldn't get database %s. Here's why: %s: %s", name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['Database']


def delete_table(client, db_name, table_name):
    """
    Deletes a table from a metadata database.

    :client: The AWS Glue client.
    :param db_name: The name of the database that contains the table.
    :param table_name: The name of the table to delete.
    """
    try:
        client.delete_table(DatabaseName=db_name, Name=table_name)
    except ClientError as err:
        logger.error(
            "Couldn't delete table %s. Here's why: %s: %s", table_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise


def delete_database(client, name):
    """
    Deletes a metadata database from your Data Catalog.

    :client: The AWS Glue client.
    :param name: The name of the database to delete.
    """
    try:
        client.delete_database(Name=name)
    except ClientError as err:
        logger.error(
            "Couldn't delete database %s. Here's why: %s: %s", name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise


if __name__ == "__main__":

    # ###################################################################
    # Variable definition
    # ###################################################################

    load_dotenv('/home/user/.env')
    # General AWS variables
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    region_name = 'us-east-1'
    # S3 variables
    bucket_name = "dmsauto-nasdaq-raw"
    # Glue and S3 variables
    glue = boto3.client('glue', region_name=region_name)
    crawler_name = "crawler_dmsauto-nasdaq-raw__data"
    db_name = 'dmsauto-nasdaq-raw__data'
    role_arn='AWSGlueServiceRole-dmsauto-nasdaq'
    # Athena variables
    athena = boto3.client('athena', region_name=region_name)
    athena_bucket_name = 'dmsauto-nasdaq-athena'
    params = {
        'region': region_name,
        'database': db_name,
        'bucket': athena_bucket_name,
        'path': 'temp/athena/output',
        'query': f'SELECT * FROM "{db_name}"."data" limit 10;',
    }

    # ###################################################################
    # Glue demonstration
    # ###################################################################
    # Note: To run the crawler, you need to have the necessary permissions.
    #       For more information, see the documentation that is present in
    #       the PDF article present in this repository.

    # Check if crawler already exists
    crawler = get_crawler(client=glue, name=crawler_name)

    # If it doesn't exist, create it
    if crawler == None:
        create_crawler(
            client=glue,
            name=crawler_name,
            role_arn=role_arn,
            db_name=db_name,
            db_prefix='',
            s3_target='s3://dmsauto-nasdaq-raw/data'
            )
    
    # Run crawler
    start_crawler(client=glue, name=crawler_name)
    print("Waiting for the crawling to finish. This may take a while.")
    crawler_state = None
    while crawler_state != 'READY':
        time.sleep(10)
        # Check if the crawler is still running
        crawler = get_crawler(client=glue, name=crawler_name)
        crawler_state = crawler['State']
        print(f"Crawler is {crawler_state}.")
    print('-'*80)

    print(json.dumps(crawler, indent=4, sort_keys=True, default=str))

    # ###################################################################
    # Athena demonstration
    # ###################################################################
    
    # This function executes the query and returns the query execution ID
    query_job = athena.start_query_execution(
        QueryString = params['query'],
        QueryExecutionContext = {'Database' : "default"},
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )

    # Print query job and wait for it to finish
    print(query_job)
    time.sleep(10)

    # Get query execution status
    query_id = query_job['QueryExecutionId']
    query_exec = athena.get_query_execution(QueryExecutionId=query_id)
    status = query_exec['QueryExecution']['Status']['State']

    # Print results if query execution was successful
    if status == 'SUCCEEDED':
        location = query_exec['QueryExecution']['ResultConfiguration']['OutputLocation']
        # Function to get output results
        query_result = athena.get_query_results(QueryExecutionId=query_id)
        result_data = query_result['ResultSet']


    # ###################################################################
    # Resource deletion
    # ###################################################################

    delete_database(client=glue, name=db_name)
    delete_crawler(client=glue, name=crawler_name)
