"""
--------------------------------------------------------------
File: conn.py                           Author: Neelakanteswara
Created Date: 06-04-2025                Modified Date: 99-99-9999
--------------------------------------------------------------
Description:
    This file contains connection utility functions for integrating
    with external systems like Snowflake and AWS S3 as part of the
    Yelp Data Pipeline. The core functions include:

    1. snow_conn:
       - Establishes a secure connection to Snowflake using key-pair
         authentication.
       - Loads the private RSA key from the path specified in the
         configuration.
       - Returns an active Snowflake connection object.

    2. aws_s3_client:
       - Initializes a boto3 client for interacting with AWS S3 using
         access keys and region from the configuration file.
       - Returns the S3 client object.

    These connections are utilized by downstream pipeline stages for
    uploading data to the cloud (S3) and ingesting it into Snowflake
    for analytical processing.
--------------------------------------------------------------
"""

from utils import parse_config
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import Error
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import boto3
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError


def snow_conn(config_path: str) -> SnowflakeConnection:
    """
    Creates a Snowflake connection.

    Params:
        config_path (str): The path to the configuration file.

    Returns:
        SnowflakeConnection: A Snowflake connection object.

    Raises:
        Exception: If any error occurs during the creation of the Snowflake connection.
    """
    try:
        # Parse the configuration file to get Snowflake connection parameters
        parsed_config = parse_config(config_path)

        # Extracting Snowflake parameters from parsed_config dictionary
        snowflake_account = parsed_config['snowflake']['account']
        snowflake_user = parsed_config['snowflake']['user']
        snowflake_role = parsed_config['snowflake']['role']
        snowflake_warehouse = parsed_config['snowflake']['warehouse']
        snowflake_database = parsed_config['snowflake']['raw_database']
        rsa_private_key = parsed_config['snowflake']['private_key_file_path']

        with open(rsa_private_key, "rb") as key:
            private_key = serialization.load_pem_private_key(
                key.read(),
                password=None,
                backend=default_backend()
            )

        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Establishing Snowflake connection using extracted Snowflake parameters
        snowflake_conn = snowflake.connector.connect(
            account=snowflake_account,
            user=snowflake_user,
            private_key=private_key_bytes,
            role=snowflake_role,
            warehouse=snowflake_warehouse,
            database=snowflake_database
        )

        # Print a message indicating successful connection
        print("Snowflake connection established.")

        # Return the Snowflake connection object
        return snowflake_conn

    except Error as error:
        # Raise an exception if any error occurs
        raise Exception(f"Error occurred while establishing Snowflake connection: {error}")


def aws_s3_client(config_path: str) -> (BaseClient, str):
    """
    Creates an AWS S3 client.

    Params:
        config_path (str): The path to the configuration file.

    Returns:
        (BaseClient, str): A tuple containing the S3 client and the bucket name.

    Raises:
        Exception: If any error occurs during the creation of the S3 client.
    """
    try:
        # Parse the configuration file to get S3 connection parameters
        parsed_config = parse_config(config_path)

        # Extracting S3 parameters from parsed_config dictionary
        aws_access_key_id = parsed_config['aws']['aws_access_key_id']
        aws_secret_access_key = parsed_config['aws']['aws_secret_access_key']
        aws_region = parsed_config['aws']['aws_region']

        # Create the S3 client using the retrieved configuration parameters
        client = boto3.client(
            service_name="s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

        return client

    except (BotoCoreError, ClientError) as error:
        # Raise an exception if any error occurs
        raise Exception(f"Error creating S3 client: {error}")
