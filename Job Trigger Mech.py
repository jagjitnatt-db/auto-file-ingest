# Databricks notebook source
# MAGIC %md
# MAGIC <h4>Create Widgets

# COMMAND ----------

dbutils.widgets.text("monitored_directory", "")
dbutils.widgets.text("checkpoint_directory", "")
dbutils.widgets.text("workflow_mapping_table", "")
dbutils.widgets.text("log_table", "")
dbutils.widgets.text("batch_interval", "")

# COMMAND ----------

monitored_directory = dbutils.widgets.get("monitored_directory")
checkpoint_directory = dbutils.widgets.get("checkpoint_directory")
workflow_mapping_table = dbutils.widgets.get("workflow_mapping_table")
log_table = dbutils.widgets.get("log_table")
batch_interval = dbutils.widgets.get("batch_interval")

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Import necessary modules and broadcast credentials to use Databricks Python SDK

# COMMAND ----------

from pyspark.sql.functions import *
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import QueueSettings
host = spark.conf.get("spark.databricks.workspaceUrl")
#token = spark.sparkContext.broadcast(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
client_id = dbutils.secrets.get("e2demo", "sp-id")
client_secret = dbutils.secrets.get("e2demo", "sp-key")

#host = spark.sparkContext.broadcast(spark.conf.get("spark.databricks.workspaceUrl"))
#token = spark.sparkContext.broadcast(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())

spark.conf.set("spark.sql.files.ignoreMissingFiles", True)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Helper Functions

# COMMAND ----------

def find_directories(path: str):
  return concat(lit(monitored_directory),
    element_at(
      split(
        regexp_replace(
          path
          , monitored_directory, "")
        , "/"),
        1)
  )

def find_tables(directory: str):
  return element_at(
    split(directory,
      "/"),
    -1)

def get_existing_job(w:WorkspaceClient, job_name:str):
  print(f"Checking existence of job {job_name}")
  existing_job = list(w.jobs.list(name= job_name, limit=5))
  existing_job_id = existing_job[0].job_id
  job = w.jobs.get(job_id=existing_job_id)
  return job

def has_queued_run(w:WorkspaceClient, job_id:int):
  runs = w.jobs.list_runs(job_id=job_id, active_only=True)
  for run in runs:
    if run.state.life_cycle_state.value == 'QUEUED':
      return True
  return False

@udf("long")
def trigger_workflow(job_name:str):
  if job_name is None:
    return -1
  w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
  job_id = get_existing_job(w, job_name).job_id
  if has_queued_run(w, job_id):
    return 1
  status = w.jobs.run_now(job_id, queue=QueueSettings("true"))
  return status.run_id


# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Read all incoming files using AutoLoader

# COMMAND ----------

from pyspark.sql.functions import *

new_detected_files = (spark.readStream 
  .format("cloudFiles")
  .option("cloudFiles.format", "binaryFile")
  .option("cloudFiles.useNotifications", True)
  .option("recursiveFileLookup", "true")
  .option("cloudFiles.maxFilesPerTrigger", 100)
  .option("cloudFiles.connectionString", "")
  .option("cloudFiles.subscriptionId", "")
  .option("cloudFiles.resourceGroup", "")
  .option("cloudFiles.tenantId", "")
  .option("cloudFiles.queueName", "")
  .load(monitored_directory).withColumn("path", regexp_replace("path", "dbfs:", ""))
  .withColumn("directory",find_directories("path"))
  .withColumn("table_name", find_tables("directory"))
  .select("directory", "table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Read the mapping table and filter out Inactive entries

# COMMAND ----------

def process_batch(df, batchId):
  batch_df = df.distinct()
  mapping = spark.read.table(workflow_mapping_table).filter("active_ind = 'Y'").select("directory", "workflow_name")
  triggered_jobs = (batch_df.join(mapping, 'directory', "left")
                  .withColumn("run_id", trigger_workflow(col("workflow_name")))
                  .withColumn("job_trigger_time", current_timestamp()))
  triggered_jobs.write.option("txnVersion", batchId).option("txnAppId", "Job_Orchestrator").mode("append").saveAsTable(log_table)

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Trigger the jobs using UDF</h4>
# MAGIC Missing entries in mapping table will be ignored and their run_id will be loaded in log table as -1

# COMMAND ----------

#new_detected_files.writeStream.foreachBatch(process_batch).option("checkpointLocation", checkpoint_directory).trigger(processingTime=batch_interval).start()
new_detected_files.writeStream.foreachBatch(process_batch).option("checkpointLocation", checkpoint_directory).start()
