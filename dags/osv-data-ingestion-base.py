#!/usr/bin/env python3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import time
import logging
import zipfile
import json
import shutil
import subprocess
import pandas as pd
from azure.storage.blob import BlobServiceClient
import hashlib

# ---------------------- Custom Logger Configuration ----------------------
LOG_FILE = os.path.join(os.path.dirname(__file__), "osv_data_ingestion.log")
custom_logger = logging.getLogger("osv_data_ingestion")
custom_logger.setLevel(logging.INFO)
if not custom_logger.handlers:
    fh = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    custom_logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    custom_logger.addHandler(sh)

# ---------------------- Helper Functions ----------------------
# Function to load configuration from a JSON file
def load_config(config_file="config.json"): 
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        #custom_logger.info(f"Configuration loaded from {config_file}.")
        return config
    except Exception as e:
        custom_logger.error(f"Error loading config file {config_file}: {e}")
        raise
# Function to compute SHA256 hash of a file
def compute_file_hash(file_path):
    """Compute SHA256 hash of a file."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()
# Function to load previously processed files and their hashes
def load_processed_files():
    """Load previously processed files and their hashes."""
    config = load_config()
    TRACKING_FILE=config.get("TRACKING_FILE")
    if os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, "r") as f:
            return json.load(f)
    return {}
# Function to save the processed files and their hashes
def save_processed_files(processed_dict):
    """Save the processed files and their hashes."""
    config = load_config()
    TRACKING_FILE=config.get("TRACKING_FILE")
    with open(TRACKING_FILE, "w") as f:
        json.dump(processed_dict, f, indent=4)

# Function to download a file from a URL and save it locally
def download_file(url, local_filename, max_retries, retry_delay, timeout=30): 
    attempt = 0
    while attempt < max_retries:
        try:
            custom_logger.info(f"Downloading {url} (attempt {attempt+1})")
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            custom_logger.info(f"Successfully downloaded: {local_filename}")
            return True
        except Exception as e:
            attempt += 1
            custom_logger.error(f"Error downloading {url}: {e}")
            time.sleep(retry_delay)
    custom_logger.error(f"Failed to download {url} after {max_retries} attempts")
    return False

# Function to extract a ZIP file
def extract_zip(local_file, extract_dir):
    try:
        with zipfile.ZipFile(local_file, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        custom_logger.info(f"Extracted {local_file} to {extract_dir}")
        return True
    except Exception as e:
        custom_logger.error(f"Failed to extract {local_file}: {e}")
        return False

# Function to validate a JSON file
def validate_json_file(file_path):
    required_fields = ["id", "aliases", "affected", "modified"]
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        missing = [field for field in required_fields if field not in data]
        if missing:
            return False, f"Missing fields: {', '.join(missing)}"
        return True, "Valid"
    except Exception as e:
        return False, str(e)

# Function to convert JSON files to Parquet format

def convert_json_to_parquet(source_folder, parquet_folder):
    os.makedirs(parquet_folder, exist_ok=True)
    processed_files = load_processed_files()  # Load existing tracking data

    for root, _, files in os.walk(source_folder):
        for file in files:
            if file.endswith(".json"):
                json_path = os.path.join(root, file)
                parquet_path = os.path.join(parquet_folder, file.replace(".json", ".parquet"))

                # Compute new file hash
                new_hash = compute_file_hash(json_path)

                # Skip if file is already processed and unchanged
                if file in processed_files and processed_files[file] == new_hash:
                    custom_logger.info(f"Skipping unchanged file: {json_path}")
                    continue

                try:
                    with open(json_path, "r") as f:
                        data = json.load(f)

                    # Handle JSON structures
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, dict):
                        df = pd.json_normalize(data)
                    else:
                        raise ValueError(f"Unexpected JSON format in {json_path}")

                    df.to_parquet(parquet_path, index=False)
                    custom_logger.info(f"Successfully converted {json_path} to {parquet_path}")

                    # Update tracking file
                    processed_files[file] = new_hash
                    save_processed_files(processed_files)

                except Exception as e:
                    custom_logger.error(f"Failed to convert {json_path} to Parquet: {e}")



# Function to create an Azure Storage container if it does not exist
def create_container_if_not_exists(container_name, connection_string):
    cmd = [
        "az", "storage", "container", "create",
        "--name", container_name,
        "--connection-string", connection_string
    ]
    custom_logger.info(f"Ensuring container '{container_name}' exists...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        custom_logger.info(f"Container creation output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        if "already exists" in e.stderr:
            custom_logger.info(f"Container '{container_name}' already exists.")
            return True
        else:
            custom_logger.error(f"Failed to create container '{container_name}': {e.stderr}")
            return False

# Function to upload a folder of Parquet files to Azure Storage
def upload_parquet_to_azure(source_folder, container_name, connection_string):
    processed_files = load_processed_files()
    config=load_config()
    STAGING_FOLDER = config.get("STAGING_FOLDER")   # Ensure staging directory exists
    os.makedirs(STAGING_FOLDER, exist_ok=True)

    new_files = False  # Flag to check if there are new files

    # Move only new/modified Parquet files to staging
    for root, _, files in os.walk(source_folder):
        for file in files:
            if file.endswith(".parquet"):
                parquet_path = os.path.join(root, file)
                new_hash = compute_file_hash(parquet_path)

                # Skip if already uploaded and unchanged
                if file in processed_files and processed_files[file] == new_hash:
                    custom_logger.info(f"Skipping already uploaded file: {parquet_path}")
                    continue
                
                # Copy new/changed files to the staging folder
                shutil.copy(parquet_path, os.path.join(STAGING_FOLDER, file))
                new_files = True

                # Update tracking file
                processed_files[file] = new_hash

    # If there are new files, perform batch upload
    if new_files:
        cmd = [
            "az", "storage", "blob", "upload-batch",
            "--destination", container_name,
            "--source", STAGING_FOLDER,
            "--connection-string", connection_string
        ]
        custom_logger.info(f"Uploading new/modified files in batch...")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            custom_logger.info(f"Batch upload successful: {result.stdout}")

            # Save processed files after successful upload
            save_processed_files(processed_files)

        except subprocess.CalledProcessError as e:
            custom_logger.error(f"Batch upload failed: {e.stderr}")

    else:
        custom_logger.info("No new files to upload.")

    # Clean up staging folder
    shutil.rmtree(STAGING_FOLDER)



# ---------------------- Task Functions ----------------------

# Function to download data from the specified URLs
def download_task(**kwargs):
    config = load_config()
    BASE_URL = config.get("BASE_URL")
    DATA_SOURCES = config.get("DATA_SOURCES", {})
    DOWNLOAD_DIR = config.get("DOWNLOAD_DIR", "osv_data")
    MAX_RETRIES = config.get("MAX_RETRIES", 3)
    RETRY_DELAY = config.get("RETRY_DELAY", 5)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    for ecosystem, relative_path in DATA_SOURCES.items():
        url = BASE_URL + relative_path
        local_zip = os.path.join(DOWNLOAD_DIR, f"{ecosystem}_all.zip")
        if not download_file(url, local_zip, MAX_RETRIES, RETRY_DELAY):
            custom_logger.error(f"Failed to download data for {ecosystem}.")

# Function to extract the downloaded ZIP files
def extract_task(**kwargs):
    config = load_config()
    DATA_SOURCES = config.get("DATA_SOURCES", {})
    DOWNLOAD_DIR = config.get("DOWNLOAD_DIR", "osv_data")
    for ecosystem in DATA_SOURCES.keys():
        extract_dir = os.path.join(DOWNLOAD_DIR, ecosystem)
        local_zip = os.path.join(DOWNLOAD_DIR, f"{ecosystem}_all.zip")
        os.makedirs(extract_dir, exist_ok=True)
        extract_zip(local_zip, extract_dir)

# Function to validate and convert JSON files to Parquet format

def validate_and_convert_task(**kwargs):
    config = load_config()
    DATA_SOURCES = config.get("DATA_SOURCES", {})
    DOWNLOAD_DIR = config.get("DOWNLOAD_DIR", "osv_data")

    failed_files = []  # Store invalid JSON files

    for ecosystem in DATA_SOURCES.keys():
        extract_dir = os.path.join(DOWNLOAD_DIR, ecosystem)
        parquet_dir = os.path.join(DOWNLOAD_DIR, f"{ecosystem}_parquet")
        os.makedirs(parquet_dir, exist_ok=True)

        for root, _, files in os.walk(extract_dir):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    parquet_file = os.path.join(parquet_dir, file.replace(".json", ".parquet"))

                    # Skip if already converted
                    if os.path.exists(parquet_file):
                        custom_logger.info(f"Skipping already converted file: {file_path}")
                        continue

                    # Validate JSON
                    valid, error_msg = validate_json_file(file_path)
                    if not valid:
                        custom_logger.error(f"Invalid JSON: {file_path}, Error: {error_msg}")
                        failed_files.append(file_path)
                        continue  # Skip conversion for this file

                    # Convert valid JSON
                    convert_json_to_parquet(extract_dir, parquet_dir)

    # Final log summary
    if failed_files:
        custom_logger.warning(f"Some JSON files failed validation ({len(failed_files)} total). Check logs for details.")
    else:
        custom_logger.info(" All JSON files validated and converted successfully.")

# Function to upload Parquet files to Azure Storage
def upload_task(**kwargs):
    config = load_config()
    AZURE_CONNECTION_STRING = config.get("AZURE_STORAGE_CONNECTION_STRING") #   Load Azure Storage connection string
    AZURE_CONTAINER_NAME = config.get("AZURE_CONTAINER_NAME") # Load Azure Storage container name
    if not create_container_if_not_exists(AZURE_CONTAINER_NAME, AZURE_CONNECTION_STRING):
        raise Exception("Container creation failed.")
    DATA_SOURCES = config.get("DATA_SOURCES", {})
    DOWNLOAD_DIR = config.get("DOWNLOAD_DIR", "osv_data")
    for ecosystem in DATA_SOURCES.keys():
        parquet_dir = os.path.join(DOWNLOAD_DIR, f"{ecosystem}_parquet")
        upload_parquet_to_azure(parquet_dir, AZURE_CONTAINER_NAME, AZURE_CONNECTION_STRING)

# ---------------------- Airflow DAG Definition ----------------------
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 24),
}
# Define the DAG
with DAG(
    'osv_data_ingestion',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:
    
    download_op = PythonOperator(
        task_id='download_task',
        python_callable=download_task,
        provide_context=True
    )
    
    extract_op = PythonOperator(
        task_id='extract_task',
        python_callable=extract_task,
        provide_context=True
    )
    
    validate_and_convert_op = PythonOperator(
        task_id='validate_and_convert_task',
        python_callable=validate_and_convert_task,
        provide_context=True
    )
    
    upload_op = PythonOperator(
        task_id='upload_task',
        python_callable=upload_task,
        provide_context=True
    )

    # Define task dependencies
    download_op >> extract_op >> validate_and_convert_op >> upload_op
