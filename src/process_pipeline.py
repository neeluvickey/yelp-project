"""
--------------------------------------------------------------
File: process_pipeline.py           Author: Neelakanteswara
Created Date: 12-02-2025            Modified Date: 99-99-9999
--------------------------------------------------------------
Description:
    This script serves as the main orchestration engine for the
    Yelp Data Pipeline Project. It automates the complete ETL
    process using the following steps:

    1. Reads configurations from a .cfg file to extract required
       parameters like file paths, AWS and Snowflake credentials.
    2. Iterates through all raw Yelp JSON files located in the
       staging input directory.
    3. Splits large JSON files into smaller chunks using line count.
    4. Uploads the resulting split files to AWS S3, preserving folder
       structure.
    5. Loads the uploaded JSON files into Snowflake using an external
       stage and dynamic table creation logic.

    This file is the entry point of the data pipeline and is designed
    to be modular, readable, and scalable for large dataset processing.
--------------------------------------------------------------
"""

import os
from yelp_project.src.utils import parse_config, split_json_file, upload_to_s3, data_load


def process_pipeline(config_path: str) -> None:
    """
    Main function to loop through JSON files in the staging directory,
    split them, and save the split files.

    Params:
        config_path (str): Path to the configuration file.

    Returns:
        None
    """
    # Parse configuration
    config = parse_config(config_path)

    input_dir = config["staging"]["input_dataset_path"]
    output_dir = config["staging"]["output_folder_path"]
    no_of_lines = config["staging"]["no_of_lines"]

    # Loop through all JSON files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):  # Process only JSON files
            input_file_path = os.path.join(input_dir, filename)
            print(f"Processing file: {filename}")
            split_json_file(input_file_path, output_dir, no_of_lines)

    # Upload all split files to S3 after processing
    upload_to_s3(config_path)

    # Data load to Snowflake
    data_load(config_path)


# Example usage:
if __name__ == "__main__":
    config_file_path = "yelp_project/config/pipeline_config.cfg"
    process_pipeline(config_file_path)
