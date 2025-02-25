# osv-vulnerabilities

# 📜 OSV Data Lake - Part 1: Data Ingestion  
### Automated Vulnerability Data Pipeline Using Apache Airflow & Azure  

This repository contains an **Apache Airflow-based data pipeline** to fetch, validate, process, and store **Open Source Vulnerability (OSV)** data efficiently.  

✅ **Key Features:**  
- **Automated daily ingestion** of OSV vulnerability data  
- **Schema validation** to ensure data quality  
- **Incremental processing** (avoids redundant reprocessing)  
- **Optimized storage using partitioned Parquet files**  
- **Batch upload to Azure Blob Storage** for efficient querying  
- **Logging & failure handling** with retry mechanisms  

---

## 🚀 Part 1: Data Ingestion  

🔹 **Current Focus:** **Fetching, validating, and storing OSV data** in a structured format.    

---

# 🏗 How the Solution Meets the Requirements  

## ✅ Requirement 1: Fetch Daily Updates from OSV's Public Dataset  
🔹 **Implemented:** The Airflow DAG runs **daily** (`@daily`) and fetches the latest OSV vulnerability data.  
🔹 **Code Implementation:**  
- The **`download_task`** fetches OSV data from the official OSV dataset.  
- Uses **retry logic** to handle failures in case of temporary network issues.  
- OSV data sources are **defined in `config.json`**, making it **easily configurable**.  

📌 **Code Reference:** [`osv_ingestion_base.py`](dags/osv_ingestion_base.py)  
```python
with DAG(
    'osv_data_ingestion',
    default_args=default_args,
    schedule_interval='@daily',  # Runs every day
    catchup=False,
) as dag:
    download_op = PythonOperator(
        task_id='download_task',
        python_callable=download_task,
        provide_context=True
    )
## ✅ Requirement 2: Schema Validation of Incoming Data  
🔹 **Implemented:** The JSON files are validated against a schema before processing.  
🔹 **Required Fields:**  
- `id` (Unique identifier)  
- `aliases` (Other identifiers like CVE numbers)  
- `affected` (Impacted software versions)  
- `modified` (Last modification timestamp)  

📌 **Code Reference:** [`osv_ingestion_base.py`](dags/osv_ingestion_base.py) 
```python
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
## ✅ Requirement 3: Handling Failures & Retries  
🔹 **Implemented:** The DAG **automatically retries failed tasks** to handle temporary issues.  
🔹 **Retry Mechanisms:**  
- **Download Task:** Retries 3 times with a 5-second delay between attempts.  
- **Data Extraction:** Logs errors if ZIP extraction fails and continues processing.   

📌 **Code Reference:** [`osv_ingestion_base.py`](dags/osv_ingestion_base.py)  
```python
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 24),
    'retries': 3,  # Retries failed tasks 3 times
    'retry_delay': timedelta(seconds=5),
}

# ✅ Requirement 4: Audit Logs for Ingestion Activities  

🔹 **Implemented:** Every step in the DAG logs key activities to **track progress, debug failures, and maintain audit trails**.  
🔹 **Log Outputs Include:**  
- **Download status** – Logs when a file is successfully downloaded or if it fails.  
- **Extraction details** – Logs the extraction of ZIP files.  
- **Schema validation results** – Logs if files pass or fail validation.  
- **File processing updates** – Logs JSON ➝ Parquet conversion and uploads to Azure.  

📌 **Code Reference:** [`osv_ingestion_base.py`](dags/osv_ingestion_base.py)  
```python
custom_logger.info(f"Successfully downloaded: {local_filename}")
custom_logger.error(f"Error downloading {url}: {e}")
custom_logger.info(f"Extracted {local_file} to {extract_dir}")
custom_logger.error(f"Failed to extract {local_file}: {e}")
custom_logger.info(f"Converted {json_path} to {parquet_path}")
custom_logger.error(f"Failed to convert {json_path} to Parquet: {e}")

# ✅ Requirement 5: Incremental Processing (Avoiding Redundant Work)  

## 🔹 Overview  
The OSV Data Lake ingestion pipeline ensures that **only new or modified files are processed** instead of reprocessing all files daily. This significantly improves **performance** and **reduces redundant computation**.

## 🔹 How Incremental Processing Works  
1. **A SHA256 hash is computed** for each JSON file before processing.  
2. If the **hash matches the last processed version**, the file is **skipped**.  
3. If the file is **new or modified**, it is processed and stored in Azure.  
4. The **hash is saved in `processed_files.json`**, ensuring future runs detect changes efficiently.  


📌 **Code Reference:** [`osv_ingestion_base.py`](dags/osv_ingestion_base.py)  
```python
import hashlib
import json
import os

def load_processed_files():
    """Load previously processed files and their hashes."""
    if os.path.exists(PROCESSED_FILES_TRACKER):
        with open(PROCESSED_FILES_TRACKER, "r") as f:
            return json.load(f)
    return {}

def save_processed_files(processed_files):
    """Save processed file hashes for future DAG runs."""
    with open(PROCESSED_FILES_TRACKER, "w") as f:
        json.dump(processed_files, f, indent=4)



