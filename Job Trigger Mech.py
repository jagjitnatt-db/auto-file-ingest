# Databricks notebook source
# MAGIC %md
# MAGIC <h4>Create Widgets

# COMMAND ----------

# Create Databricks widgets for all configurable parameters
# These allow users to set values from the Databricks UI or API

dbutils.widgets.text("monitored_directory", "")
dbutils.widgets.text("checkpoint_directory", "")
dbutils.widgets.text("workflow_mapping_table", "")
dbutils.widgets.text("log_table", "")
dbutils.widgets.text("batch_interval", "")
dbutils.widgets.text("cloudFiles_format", "binaryFile")
dbutils.widgets.text("cloudFiles_useNotifications", "True")
dbutils.widgets.text("cloudFiles_maxFilesPerTrigger", "100")
dbutils.widgets.text("cloudFiles_connectionString", "")
dbutils.widgets.text("cloudFiles_subscriptionId", "")
dbutils.widgets.text("cloudFiles_resourceGroup", "")
dbutils.widgets.text("cloudFiles_tenantId", "")
dbutils.widgets.text("cloudFiles_queueName", "")

# COMMAND ----------

# Retrieve widget values for use in the script
monitored_directory = dbutils.widgets.get("monitored_directory")
checkpoint_directory = dbutils.widgets.get("checkpoint_directory")
workflow_mapping_table = dbutils.widgets.get("workflow_mapping_table")
log_table = dbutils.widgets.get("log_table")
batch_interval = dbutils.widgets.get("batch_interval")
# Use default if widget is empty
cloudFiles_useNotifications = dbutils.widgets.get("cloudFiles_useNotifications")
cloudFiles_maxFilesPerTrigger = dbutils.widgets.get("cloudFiles_maxFilesPerTrigger") or 25
cloudFiles_connectionString = dbutils.widgets.get("cloudFiles_connectionString")
cloudFiles_subscriptionId = dbutils.widgets.get("cloudFiles_subscriptionId")
cloudFiles_resourceGroup = dbutils.widgets.get("cloudFiles_resourceGroup")
cloudFiles_tenantId = dbutils.widgets.get("cloudFiles_tenantId")
cloudFiles_queueName = dbutils.widgets.get("cloudFiles_queueName")

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Import necessary modules and broadcast credentials to use Databricks Python SDK

# COMMAND ----------

from pyspark.sql.functions import *
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import QueueSettings
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F

# Retrieve Databricks workspace host and credentials from secrets/config
host = spark.conf.get("spark.databricks.workspaceUrl")
client_id = dbutils.secrets.get("e2demo", "sp-id")
client_secret = dbutils.secrets.get("e2demo", "sp-key")

# Set Spark and Delta Lake configuration for file handling and optimization
spark.conf.set("spark.sql.files.ignoreMissingFiles", True)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Helper Functions

# COMMAND ----------

def find_directories(path: str):
    """Extract the immediate subdirectory from the monitored directory path."""
    return concat(lit(monitored_directory),
        element_at(
            split(
                regexp_replace(
                    path, monitored_directory, ""),
                "/"),
            1)
    )

def find_tables(directory: str):
    """Extract the table name from the directory path (last segment)."""
    return element_at(
        split(directory, "/"),
        -1)

def get_existing_job(w:WorkspaceClient, job_name:str):
    """Return the job object for a given job name, or None if not found. Handles errors."""
    try:
        print(f"Checking existence of job {job_name}")
        existing_job = list(w.jobs.list(name= job_name, limit=5))
        if not existing_job:
            print(f"No job found with name {job_name}")
            return None
        existing_job_id = existing_job[0].job_id
        job = w.jobs.get(job_id=existing_job_id)
        return job
    except Exception as e:
        print(f"Error fetching job {job_name}: {e}")
        return None

def has_queued_run(w:WorkspaceClient, job_id:int):
    """Check if there is a queued run for the given job ID. Handles errors."""
    try:
        runs = w.jobs.list_runs(job_id=job_id, active_only=True)
        for run in runs:
            if run.state.life_cycle_state.value == 'QUEUED':
                return True
        return False
    except Exception as e:
        print(f"Error checking queued runs for job_id {job_id}: {e}")
        return False

@udf("long")
def trigger_workflow(job_name:str):
    """UDF to trigger a Databricks job by name. Returns run_id, 1 if already queued, -1 if not found, -2 on error."""
    if job_name is None:
        return -1
    try:
        w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        job = get_existing_job(w, job_name)
        if job is None:
            return -1
        job_id = job.job_id
        if has_queued_run(w, job_id):
            return 1
        status = w.jobs.run_now(job_id, queue=QueueSettings("true"))
        return status.run_id
    except Exception as e:
        print(f"Error triggering workflow for job {job_name}: {e}")
        return -2

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Read all incoming files using AutoLoader

# COMMAND ----------

# Set up a streaming read using AutoLoader to detect new files in the monitored directory
# All cloudFiles options are parameterized via widgets
new_detected_files = (spark.readStream 
    .format("cloudFiles")
    .option("cloudFiles.useNotifications", cloudFiles_useNotifications)
    .option("recursiveFileLookup", "true")
    .option("cloudFiles.maxFilesPerTrigger", cloudFiles_maxFilesPerTrigger)
    .option("cloudFiles.connectionString", cloudFiles_connectionString)
    .option("cloudFiles.subscriptionId", cloudFiles_subscriptionId)
    .option("cloudFiles.resourceGroup", cloudFiles_resourceGroup)
    .option("cloudFiles.tenantId", cloudFiles_tenantId)
    .option("cloudFiles.queueName", cloudFiles_queueName)
    .load(monitored_directory)
    .withColumn("path", regexp_replace("path", "dbfs:", ""))
    .withColumn("directory", find_directories("path"))
    .withColumn("table_name", find_tables("directory"))
    .select("directory", "table_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Read the mapping table and filter out Inactive entries

# COMMAND ----------

# Remove mapping_schema and initial_data from global scope, as they are not needed outside deploy_framework()

def deploy_framework(workflow_mapping_table:str):
    """
    Deploys the workflow mapping table if it does not exist.
    This function creates the table with the required schema using SQL and displays its contents.
    """
    # (Optional) If you want to keep a schema definition for reference, define it here, but it's not used for SQL table creation
    # mapping_schema = StructType([...])
    # initial_data = [...]
    spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {workflow_mapping_table} (
        directory STRING NOT NULL,
        workflow_name STRING NOT NULL,
        active_ind STRING NOT NULL,
        last_change_user STRING,
        last_updated_ts TIMESTAMP
    ) USING DELTA
    ''')
    print(f"Ensured table {workflow_mapping_table} exists.")


def process_batch(df, batchId):
    """Process each micro-batch: join with mapping, trigger jobs, and log results. Handles errors."""
    try:
        batch_df = df.distinct()  # Remove duplicate file events
        # Read mapping table and filter for active workflows
        mapping = spark.read.table(workflow_mapping_table).filter("active_ind = 'Y'").select("directory", "workflow_name")
        # Join detected files with mapping and trigger jobs
        triggered_jobs = (batch_df.join(mapping, 'directory', "left")
                        .withColumn("run_id", trigger_workflow(col("workflow_name")))
                        .withColumn("job_trigger_time", current_timestamp()))
        # Write results to log table
        triggered_jobs.write.option("txnVersion", batchId).option("txnAppId", "Job_Orchestrator").mode("append").saveAsTable(log_table)
    except Exception as e:
        print(f"Error in process_batch (batchId={batchId}): {e}")
        # Optionally, write error details to a log table or external system

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Trigger the jobs using UDF</h4>
# MAGIC Missing entries in mapping table will be ignored and their run_id will be loaded in log table as -1

# COMMAND ----------

# Start the streaming query to process new files and trigger jobs
# Note: batch_interval is defined but not used in the trigger (can be added if needed)
# new_detected_files.writeStream.foreachBatch(process_batch).option("checkpointLocation", checkpoint_directory).trigger(processingTime=batch_interval).start()
new_detected_files.writeStream.foreachBatch(process_batch).option("checkpointLocation", checkpoint_directory).start()
