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
🔹 **Upcoming Parts:** Querying, Analytics, and Visualization (Future).  

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
