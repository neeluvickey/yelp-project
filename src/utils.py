"""
--------------------------------------------------------------
File: utils.py                       Author: Neelakanteswara
Created Date: 16-04-2025             Modified Date: 99-99-9999
--------------------------------------------------------------
Description:
    This file contains core utility functions used in the Yelp
    Data Pipeline for configuration parsing, data preprocessing,
    cloud uploading, and Snowflake data loading. The key functionality
    includes:

    1. parse_config:
       - Reads and parses values from the pipeline configuration file
         using configparser.
       - Returns structured configuration details for staging, AWS, and
         Snowflake.

    2. split_json_file:
       - Splits large Yelp JSON datasets into smaller JSON files based
         on a configurable number of lines.
       - Stores split files under categorized folders for easy access
         and organization.

    3. upload_to_s3:
       - Uploads split files from local storage to Amazon S3, maintaining
         their sub-folder structure.
       - Utilizes concurrent uploading to enhance performance using
         ThreadPoolExecutor.

    4. data_load:
       - Creates external stages in Snowflake using S3 credentials.
       - Dynamically creates target tables in Snowflake if not already
         present.
       - Loads JSON data from S3 into respective Snowflake tables with
         `STRIP_OUTER_ARRAY = TRUE` option.

    These functions form the backbone of the ETL pipeline and are
    designed for modularity, reusability, and scalability when working
    with large semi-structured datasets in a cloud-based environment.
--------------------------------------------------------------
"""

import os
import json
import shutil
import configparser
import concurrent.futures


def parse_config(config_path: str) -> dict:
    """
    Parses the configuration file using RawConfigParser and returns a dictionary with all config values.

    Params:
        config_path (str): Path to the configuration file.

    Returns:
        dict: A dictionary containing configuration values.
    """
    config = configparser.RawConfigParser()
    config.read(config_path)

    parsed_config = {
        "staging": {
            "input_dataset_path": config.get("STAGING", "input_dataset_path"),
            "output_folder_path": config.get("STAGING", "output_folder_path"),
            "no_of_lines": config.getint("STAGING", "no_of_lines"),
        },
        "aws": {
            "aws_access_key_id": config.get("AWS", "aws_access_key_id"),
            "aws_secret_access_key": config.get("AWS", "aws_secret_access_key"),
            "aws_region": config.get("AWS", "aws_region"),
            "s3_bucket_name": config.get("AWS", "s3_bucket_name"),
            "s3_bucket_path": config.get("AWS", "s3_bucket_path"),
        },
        "snowflake": {
            "private_key_file_path": config.get("SNOWFLAKE", "private_key_file_path"),
            "raw_database": config.get("SNOWFLAKE", "raw_database"),
            "raw_schema": config.get("SNOWFLAKE", "raw_schema"),
            "warehouse": config.get("SNOWFLAKE", "warehouse"),
            "account": config.get("SNOWFLAKE", "account"),
            "role": config.get("SNOWFLAKE", "role"),
            "user": config.get("SNOWFLAKE", "user"),
        }
    }

    return parsed_config


def split_json_file(input_file: str, output_dir: str, no_of_lines: int) -> None:
    """
    Splits a large JSON file into smaller files based on the specified number of lines.

    Params:
        input_file (str): Path to the input JSON file.
        output_dir (str): Directory where the split files will be saved.
        no_of_lines (int): Number of lines per split file.

    Returns:
        None
    """
    # Extract the last word from the filename
    base_name = os.path.basename(input_file).replace(".json", "")
    split_key = base_name.split("_")[-1]  # Extracts "business" from "yelp_academic_dataset_business"

    # Define the final output directory
    final_output_dir = os.path.join(output_dir, split_key)

    # Clear existing files in the directory
    if os.path.exists(final_output_dir):
        shutil.rmtree(final_output_dir)  # Deletes the folder and its contents

    os.makedirs(final_output_dir, exist_ok=True)  # Recreate an empty folder

    with open(input_file, "r", encoding="utf-8") as infile:
        data = [json.loads(line) for line in infile]  # Read file as a list of JSON objects

    total_docs = len(data)
    file_count = 1

    for i in range(0, total_docs, no_of_lines):
        split_data = data[i:i + no_of_lines]
        output_filename = f"{split_key}_{len(split_data)}_part_{file_count}.json"
        output_path = os.path.join(final_output_dir, output_filename)

        with open(output_path, "w", encoding="utf-8") as outfile:
            json.dump(split_data, outfile, indent=4)

        file_count += 1


def upload_file_to_s3(s3_client, bucket_name, s3_key, local_file_path):
    """
    Upload a single file to S3.

    Params:
        s3_client: Boto3 S3 client
        bucket_name (str): S3 bucket name
        s3_key (str): S3 key (path inside bucket)
        local_file_path (str): Path to the local file

    Returns:
        None
    """
    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"✅ Uploaded: {local_file_path} -> s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Error uploading {local_file_path}: {e}")


def upload_to_s3(config_path: str, max_workers=10) -> None:
    """
    Uploads split files from the output directory to S3 in parallel, maintaining folder structure.

    Params:
        config_path (str): Path to the configuration file.
        max_workers (int): Number of parallel upload threads (default: 10)

    Returns:
        None
    """
    from yelp_project.src.conn import aws_s3_client

    # Parse configuration
    config = parse_config(config_path)

    local_dir = config["staging"]["output_folder_path"]  # Directory containing split files
    bucket_name = config["aws"]["s3_bucket_name"]  # S3 bucket name
    s3_bucket_path = config["aws"]["s3_bucket_path"].rstrip("/")  # Remove trailing "/"

    # Extract prefix after "s3://bucket-name/"
    s3_prefix = "/".join(s3_bucket_path.split("/")[3:])  # This gives "dataset"

    # Get S3 client from conn.py
    s3_client = aws_s3_client(config_path)

    # Collect all files for upload
    upload_tasks = []
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            local_file_path = str(local_file_path)

            # Extract relative path to maintain folder structure
            relative_path = os.path.relpath(local_file_path, local_dir).replace("\\", "/")
            s3_key = f"{s3_prefix}/{relative_path}".lstrip("/")  # Ensure no leading "/"

            upload_tasks.append((s3_client, bucket_name, s3_key, local_file_path))

    # Use ThreadPoolExecutor to upload in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda args: upload_file_to_s3(*args), upload_tasks)

    print("✅ All files uploaded successfully!")


def data_load(config_path: str) -> None:
    """
    Loads data from S3 to Snowflake.
    - Creates an external stage using S3 credentials.
    - Creates a table if not exists.
    - Loads data from S3 into the Snowflake table with STRIP_OUTER_ARRAY = TRUE.

    Params:
        config_path (str): Path to the configuration file.

    Returns:
        None
    """
    from yelp_project.src.conn import snow_conn

    # Parse configuration
    config = parse_config(config_path)

    # Snowflake details
    database = config["snowflake"]["raw_database"]
    schema = config["snowflake"]["raw_schema"]
    stage_name = f"{database}.{schema}.S3_STAGE"

    # AWS S3 details
    s3_base_path = config["aws"]["s3_bucket_path"].rstrip("/")
    aws_access_key = config["aws"]["aws_access_key_id"]
    aws_secret_key = config["aws"]["aws_secret_access_key"]

    # Get Snowflake connection from conn.py
    conn = snow_conn(config_path)
    cur = conn.cursor()

    # Create external stage using S3 credentials
    create_stage_query = f"""
    CREATE STAGE IF NOT EXISTS {stage_name}
    URL = '{s3_base_path}'
    CREDENTIALS = (AWS_KEY_ID = '{aws_access_key}' AWS_SECRET_KEY = '{aws_secret_key}')
    FILE_FORMAT = (TYPE = 'JSON')
    """
    cur.execute(create_stage_query)
    print(f"Stage {stage_name} is ready.")

    # Loop through split files and load data
    local_dir = config["staging"]["output_folder_path"]

    for root, _, files in os.walk(local_dir):
        for file in files:
            if file.endswith(".json"):
                # Extract the root filename (e.g., "business" from "business_1.json")
                table_name = os.path.basename(root)

                # Create table if not exists
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
                    data VARIANT
                )
                """
                cur.execute(create_table_query)
                print(f"Table {table_name} is ready.")

                # Copy data from S3 to Snowflake table with STRIP_OUTER_ARRAY = TRUE
                copy_query = f"""
                COPY INTO {database}.{schema}.{table_name}
                FROM @{stage_name}/{table_name}/
                FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE)
                """
                cur.execute(copy_query)
                print(f"Data from {file} loaded into {table_name}.")

    # Close connection
    cur.close()
    conn.close()
    print("Data load process completed.")
