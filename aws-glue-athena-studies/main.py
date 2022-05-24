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


# class GlueWrapper
#     def __init__(self, glue_client)

#     def get_crawler(self, name)
#     def create_crawler(self, name, role_arn, db_name, db_prefix, s3_target)
#     def start_crawler(self, name)
#     def delete_crawler(self, name)

#     def get_database(self, name)
#     def delete_database(self, name)

#     def get_tables(self, db_name)
#     def delete_table(self, db_name, table_name)

#     def create_job(self, name, description, role_arn, script_location)
#     def start_job_run(self, name, input_database, input_table, output_bucket_name)
#     def list_jobs(self)
#     def delete_job(self, job_name)

#     def get_job_runs(self, job_name)
#     def get_job_run(self, name, run_id)











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


def get_database(client, name):
    """
    Gets information about a database in your Data Catalog.

    :param name: The name of the database to look up.
    :return: Information about the database.
    """
    try:
        response = client.get_database(Name=name)
    except ClientError as err:
        logger.error(
            "Couldn't get database %s. Here's why: %s: %s", name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['Database']


def get_tables(client, db_name):
    """
    Gets a list of tables in a Data Catalog database.

    :param db_name: The name of the database to query.
    :return: The list of tables in the database.
    """
    try:
        response = client.get_tables(DatabaseName=db_name)
    except ClientError as err:
        logger.error(
            "Couldn't get tables %s. Here's why: %s: %s", db_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['TableList']


def create_job(client, name, description, role_arn, script_location):
    """
    Creates a job definition for an extract, transform, and load (ETL) job that can
    be run by AWS Glue.

    :param name: The name of the job definition.
    :param description: The description of the job definition.
    :param role_arn: The ARN of an IAM role that grants AWS Glue the permissions
                        it requires to run the job.
    :param script_location: The Amazon S3 URL of a Python ETL script that is run as
                            part of the job. The script defines how the data is
                            transformed.
    """
    try:
        client.create_job(
            Name=name, Description=description, Role=role_arn,
            Command={'Name': 'glueetl', 'ScriptLocation': script_location, 'PythonVersion': '3'},
            GlueVersion='3.0')
    except ClientError as err:
        logger.error(
            "Couldn't create job %s. Here's why: %s: %s", name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise


def start_job_run(client, name, input_database, input_table, output_bucket_name):
    """
    Starts a job run. A job run extracts data from the source, transforms it,
    and loads it to the output bucket.

    :param name: The name of the job definition.
    :param input_database: The name of the metadata database that contains tables
                            that describe the source data. This is typically created
                            by a crawler.
    :param input_table: The name of the table in the metadata database that
                        describes the source data.
    :param output_bucket_name: The S3 bucket where the output is written.
    :return: The ID of the job run.
    """
    try:
        # The custom Arguments that are passed to this function are used by the
        # Python ETL script to determine the location of input and output data.
        response = client.start_job_run(
            JobName=name,
            Arguments={
                '--input_database': input_database,
                '--input_table': input_table,
                '--output_bucket_url': f's3://{output_bucket_name}/'})
    except ClientError as err:
        logger.error(
            "Couldn't start job run %s. Here's why: %s: %s", name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['JobRunId']


def list_jobs(client):
    """
    Lists the names of job definitions in your account.

    :return: The list of job definition names.
    """
    try:
        response = client.list_jobs()
    except ClientError as err:
        logger.error(
            "Couldn't list jobs. Here's why: %s: %s",
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['JobNames']


def get_job_runs(client, job_name):
    """
    Gets information about runs that have been performed for a specific job
    definition.

    :param job_name: The name of the job definition to look up.
    :return: The list of job runs.
    """
    try:
        response = client.get_job_runs(JobName=job_name)
    except ClientError as err:
        logger.error(
            "Couldn't get job runs for %s. Here's why: %s: %s", job_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['JobRuns']


def get_job_run(client, name, run_id):
    """
    Gets information about a single job run.

    :param name: The name of the job definition for the run.
    :param run_id: The ID of the run.
    :return: Information about the run.
    """
    try:
        response = client.get_job_run(JobName=name, RunId=run_id)
    except ClientError as err:
        logger.error(
            "Couldn't get job run %s/%s. Here's why: %s: %s", name, run_id,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise
    else:
        return response['JobRun']


def delete_job(client, job_name):
    """
    Deletes a job definition. This also deletes data about all runs that are
    associated with this job definition.

    :param job_name: The name of the job definition to delete.
    """
    try:
        client.delete_job(JobName=job_name)
    except ClientError as err:
        logger.error(
            "Couldn't delete job %s. Here's why: %s: %s", job_name,
            err.response['Error']['Code'], err.response['Error']['Message'])
        raise


def delete_table(client, db_name, table_name):
    """
    Deletes a table from a metadata database.

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

    :param name: The name of the database to delete.
    """
    try:
        client.delete_database(Name=name)
    except ClientError as err:
        logger.error(
            "Couldn't delete database %s. Here's why: %s: %s", name,
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



class GlueCrawler:
    """
    Encapsulates a scenario that shows how to create an AWS Glue crawler and job and use
    them to transform data from CSV to JSON format.
    """
    def __init__(self, glue_client, glue_service_role, glue_bucket):
        """
        :param glue_client: A Boto3 AWS Glue client.
        :param glue_service_role: An AWS Identity and Access Management (IAM) role
                                  that AWS Glue can assume to gain access to the
                                  resources it requires.
        :param glue_bucket: An S3 bucket that can hold a job script and output data
                            from AWS Glue job runs.
        """
        self.glue_client = glue_client
        self.glue_service_role = glue_service_role
        self.glue_bucket = glue_bucket

    @staticmethod
    def wait(seconds, tick=12):
        """
        Waits for a specified number of seconds, while also displaying an animated
        spinner.

        :param seconds: The number of seconds to wait.
        :param tick: The number of frames per second used to animate the spinner.
        """
        progress = '|/-\\'
        waited = 0
        while waited < seconds:
            for frame in range(tick):
                sys.stdout.write(f"\r{progress[frame % len(progress)]}")
                sys.stdout.flush()
                time.sleep(1/tick)
            waited += 1

    # def upload_job_script(self, job_script):
    #     """
    #     Uploads a Python ETL script to an S3 bucket. The script is used by the AWS Glue
    #     job to transform data.

    #     :param job_script: The relative path to the job script.
    #     """
    #     try:
    #         self.glue_bucket.upload_file(Filename=job_script, Key=job_script)
    #         print(f"Uploaded job script '{job_script}' to the example bucket.")
    #     except S3UploadFailedError as err:
    #         logger.error("Couldn't upload job script. Here's why: %s", err)
    #         raise

    def run(
            self, crawler_name, db_name, db_prefix, data_source): #, job_script, job_name
        """
        Runs the scenario. This is an interactive experience that runs at a command
        prompt and asks you for input throughout.

        :param crawler_name: The name of the crawler used in the scenario. If the
                             crawler does not exist, it is created.
        :param db_name: The name to give the metadata database created by the crawler.
        :param db_prefix: The prefix to give tables added to the database by the
                          crawler.
        :param data_source: The location of the data source that is targeted by the
                            crawler and extracted during job runs.
        :param job_script: The job script that is used to transform data during job
                           runs.
        :param job_name: The name to give the job definition that is created during the
                         scenario.
        """
        wrapper = GlueWrapper(self.glue_client)
        print(f"Checking for crawler {crawler_name}.")
        crawler = wrapper.get_crawler(crawler_name)
        if crawler is None:
            print(f"Creating crawler {crawler_name}.")
            wrapper.create_crawler(
                crawler_name, self.glue_service_role.arn, db_name, db_prefix, data_source)
            print(f"Created crawler {crawler_name}.")
            crawler = wrapper.get_crawler(crawler_name)
        print('-'*88)
        pprint(crawler)
        print('-'*88)

        print(f"When you run the crawler, it crawls data stored in {data_source} and "
              f"creates a metadata database in the AWS Glue Data Catalog that describes "
              f"the data in the data source.")
        print("In this example, the source data is in CSV format.")
        # ready = False
        # while not ready:
        #     ready = Question.ask_question(
        #         "Ready to start the crawler? (y/n) ", Question.is_yesno)
        wrapper.start_crawler(crawler_name)
        print("Let's wait for the crawler to run. This typically takes a few minutes.")
        crawler_state = None
        while crawler_state != 'READY':
            self.wait(10)
            crawler = wrapper.get_crawler(crawler_name)
            crawler_state = crawler['State']
            print(f"Crawler is {crawler['State']}.")
        print('-'*88)

        # database = wrapper.get_database(db_name)
        # print(f"The crawler created database {db_name}:")
        # pprint(database)
        # print(f"The database contains these tables:")
        # tables = wrapper.get_tables(db_name)
        # for index, table in enumerate(tables):
        #     print(f"\t{index + 1}. {table['Name']}")
        # table_index = Question.ask_question(
        #     f"Enter the number of a table to see more detail: ",
        #     Question.is_int, Question.in_range(1, len(tables)))
        # pprint(tables[table_index - 1])
        # print('-'*88)

        # print(f"Creating job definition {job_name}.")
        # wrapper.create_job(
        #     job_name, "Getting started example job.", self.glue_service_role.arn,
        #     f's3://{self.glue_bucket.name}/{job_script}')
        # print("Created job definition.")
        # print(f"When you run the job, it extracts data from {data_source}, transforms it "
        #       f"by using the {job_script} script, and loads the output into "
        #       f"S3 bucket {self.glue_bucket.name}.")
        # print("In this example, the data is transformed from CSV to JSON, and only a few "
        #       "fields are included in the output.")
        # job_run_status = None
        # if Question.ask_question(f"Ready to run? (y/n) ", Question.is_yesno):
        #     job_run_id = wrapper.start_job_run(
        #         job_name, db_name, tables[0]['Name'], self.glue_bucket.name)
        #     print(f"Job {job_name} started. Let's wait for it to run.")
        #     while job_run_status not in ['SUCCEEDED', 'STOPPED', 'FAILED', 'TIMEOUT']:
        #         self.wait(10)
        #         job_run = wrapper.get_job_run(job_name, job_run_id)
        #         job_run_status = job_run['JobRunState']
        #         print(f"Job {job_name}/{job_run_id} is {job_run_status}.")
        # print('-'*88)

        # if job_run_status == 'SUCCEEDED':
        #     print(f"Data from your job run is stored in your S3 bucket '{self.glue_bucket.name}':")
        #     try:
        #         keys = [obj.key for obj in self.glue_bucket.objects.filter(Prefix='run-')]
        #         for index, key in enumerate(keys):
        #             print(f"\t{index + 1}: {key}")
        #         lines = 4
        #         key_index = Question.ask_question(
        #             f"Enter the number of a block to download it and see the first {lines} "
        #             f"lines of JSON output in the block: ",
        #             Question.is_int, Question.in_range(1, len(keys)))
        #         job_data = io.BytesIO()
        #         self.glue_bucket.download_fileobj(keys[key_index - 1], job_data)
        #         job_data.seek(0)
        #         for _ in range(lines):
        #             print(job_data.readline().decode('utf-8'))
        #     except ClientError as err:
        #         logger.error(
        #             "Couldn't get job run data. Here's why: %s: %s",
        #             err.response['Error']['Code'], err.response['Error']['Message'])
        #         raise
        #     print('-'*88)

        # job_names = wrapper.list_jobs()
        # if job_names:
        #     print(f"Your account has {len(job_names)} jobs defined:")
        #     for index, job_name in enumerate(job_names):
        #         print(f"\t{index + 1}. {job_name}")
        #     job_index = Question.ask_question(
        #         f"Enter a number between 1 and {len(job_names)} to see the list of runs for "
        #         f"a job: ", Question.is_int, Question.in_range(1, len(job_names)))
        #     job_runs = wrapper.get_job_runs(job_names[job_index - 1])
        #     if job_runs:
        #         print(f"Found {len(job_runs)} runs for job {job_names[job_index - 1]}:")
        #         for index, job_run in enumerate(job_runs):
        #             print(
        #                 f"\t{index + 1}. {job_run['JobRunState']} on "
        #                 f"{job_run['CompletedOn']:%Y-%m-%d %H:%M:%S}")
        #         run_index = Question.ask_question(
        #             f"Enter a number between 1 and {len(job_runs)} to see details for a run: ",
        #             Question.is_int, Question.in_range(1, len(job_runs)))
        #         pprint(job_runs[run_index - 1])
        #     else:
        #         print(f"No runs found for job {job_names[job_index - 1]}")
        # else:
        #     print("Your account doesn't have any jobs defined.")
        # print('-'*88)

        # print(f"Let's clean up. During this example we created job definition '{job_name}'.")
        # if Question.ask_question(
        #         "Do you want to delete the definition and all runs? (y/n) ", Question.is_yesno):
        #     wrapper.delete_job(job_name)
        #     print(f"Job definition '{job_name}' deleted.")
        # tables = wrapper.get_tables(db_name)
        # print(f"We also created database '{db_name}' that contains these tables:")
        # for table in tables:
        #     print(f"\t{table['Name']}")
        # if Question.ask_question(
        #         "Do you want to delete the tables and the database? (y/n) ", Question.is_yesno):
        #     for table in tables:
        #         wrapper.delete_table(db_name, table['Name'])
        #         print(f"Deleted table {table['Name']}.")
        #     wrapper.delete_database(db_name)
        #     print(f"Deleted database {db_name}.")
        # print(f"We also created crawler '{crawler_name}'.")
        # if Question.ask_question(
        #         "Do you want to delete the crawler? (y/n) ", Question.is_yesno):
        #     wrapper.delete_crawler(crawler_name)
        #     print(f"Deleted crawler {crawler_name}.")
        # print('-'*88)


if __name__ == "__main__":
    load_dotenv('/home/user/.env')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = "dmsauto-nasdaq-raw"

#%%
    # tickers = ["AMZN", "AAPL", "TSLA", "GOOG", "NFLX"]
    # s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # filename = "financial_data.csv"

    # nuke_bucket(s3_client, bucket_name)
    # upload_data_to_bucket(client=s3_client, filename=filename, bucket_name=bucket_name, object_prefix="data/")
    
    # bucket_exists = check_if_bucket_exists(client=s3_client, bucket_name="dmsauto-nasdaq-athena")
    # if not bucket_exists:
    #     create_bucket(client=s3_client, bucket_name="dmsauto-nasdaq-athena")


    # glue_client = boto3.client('glue', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)    
    # crawler_name = "crawler_dmsauto-nasdaq-raw__data"
    
    # db_name = "dmsauto-nasdaq-raw__data"
    # db_prefix = "foo"
    # # AWSGlueServiceRole-dmsauto-nasdaq-raw_crawler
    # create_crawler(glue_client, crawler_name, role_arn, db_name, db_prefix, s3_target)


    # iam = boto3.resource('iam')
    # print("The first 10 roles currently in your account are:")
    # roles = list_roles(10)
    # for role in roles:
    #     print(role.name)
    # print(f"The inline policies for role {roles[0].name} are:")
    # list_policies(roles[0].name)

    # role = create_role(
    #     'demo-iam-role',
    #     []) #'lambda.amazonaws.com', 'batchoperations.s3.amazonaws.com'


    # policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
    # 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    # attach_policy(role.name, policy_arn)


    # ath_client = boto3.client('athena',
    #     region_name='us-east-1',
    #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    # scenario = GlueCrawler(
    #     boto3.client('glue', region_name='us-east-1'),
    #     boto3.resource('iam').Role("crawler_dmsauto-nasdaq-raw__data"), #args.role_name
    #     boto3.resource('s3').Bucket("dmsauto-nasdaq-raw")) #args.bucket_name

    # scenario.upload_job_script(args.job_script)
    # scenario.run(
    #     crawler_name="crawler_dmsauto-nasdaq-raw__data",
    #     db_name='doc-example-database',
    #     db_prefix='doc-example-',
    #     data_source='s3://crawler-public-us-east-1/flight/2016/csv',
    #     # job_script=args.job_script,
    #     # job_name='doc-example-job'
    #     )

    client = boto3.client('glue', region_name="us-east-1")
#%%
    # Para fazer o código abaixo rodar, tive de criar manualmente o Role "AWSGlueServiceRole-dmspseudoauto" no Console,
    # selecionando "AWS Service" como Trusted Entity e dando o nome "AWSGlueServiceRole-dmspseudoauto". No entanto, mesmo assim
    # não era criado nenhuma tabela como resultado. Isso foi resolvido quando adicionei as políticas de permissões de read/write
    # para o Bucket no IAM dentro do Role.

    # https://hands-on.cloud/working-with-aws-glue-in-python-using-boto3/#h-prerequisites
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_crawler

    response = client.create_crawler(
        Name='crawler_dmsauto-nasdaq-raw__data',
        Role='AWSGlueServiceRole-dmspseudoauto',
        DatabaseName='dmsauto-nasdaq-raw__data',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://dmsauto-nasdaq-raw/data',
                    'Exclusions': [
                        'string',
                    ],
                    'SampleSize': 3
                },
            ]
        },
        # Schedule='cron(15 12 * * ? *)',
        # SchemaChangePolicy={
        #     'UpdateBehavior': 'UPDATE_IN_DATABASE',
        #     'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        # },
        # RecrawlPolicy={
        #     'RecrawlBehavior': 'CRAWL_EVERYTHING'
        # },
        # LineageConfiguration={
        #     'CrawlerLineageSettings': 'DISABLE'
        # }
    )

    print(json.dumps(response, indent=4, sort_keys=True, default=str))
#%%
    response = client.list_crawlers()
    print(json.dumps(response, indent=4, sort_keys=True, default=str))
    
#%%
    crawler_name = "crawler_dmsauto-nasdaq-raw__data"
    client.start_crawler(Name=crawler_name)
    print("Let's wait for the crawler to run. This typically takes a few minutes.")
    crawler_state = None
    while crawler_state != 'READY':
        time.sleep(10)
        crawler = client.get_crawler(Name=crawler_name)
        crawler_state = crawler['Crawler']['State']
        print(f"Crawler is {crawler_state}.")
    print('-'*88)
    print(json.dumps(crawler, indent=4, sort_keys=True, default=str))

#%%

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#client
    
    athena = boto3.client('athena',
        region_name='us-east-1')
        # aws_access_key_id=AWS_ACCESS_KEY_ID,
        # aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    
    params = {
        'region': 'us-east-1',
        'database': 'dmsauto-nasdaq-raw__data',
        # 'database': '"mydatabase"."finance_test"',
        'bucket': 'dmsauto-nasdaq-athena',
        'path': 'temp/athena/output',
        'query': 'SELECT * FROM "dmsauto-nasdaq-raw__data"."data" limit 10;',
        # 'SELECT * FROM "mydatabase" ."zipcode" limit 10;'
    }

#%%
    ## This function executes the query and returns the query execution ID
    query_job = athena.start_query_execution(
        QueryString = params['query'],
        QueryExecutionContext = {
            'Database' : "default"
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )

    print(query_job)
#%%
    time.sleep(10)

    response_get_query_details = athena.get_query_execution(QueryExecutionId = query_job['QueryExecutionId'])
    foo = response_get_query_details['QueryExecution']
    status = response_get_query_details['QueryExecution']['Status']['State']
    if status == 'SUCCEEDED':
        location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
        ## Function to get output results
        response_query_result = athena.get_query_results(
            QueryExecutionId = query_job['QueryExecutionId']
        )
        result_data = response_query_result['ResultSet']