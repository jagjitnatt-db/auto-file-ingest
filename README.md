# Auto File Ingest Framework

## Overview

This project provides a Databricks-based framework for automatically ingesting files from a monitored directory and triggering Databricks workflows (jobs) based on directory-to-workflow mappings. Unlike the default Databricks File Arrival Trigger, which is limited to 50 triggers per workspace, this framework can efficiently monitor thousands of sub-directories and trigger different jobs for each, as defined in the mapping table. It is designed for scalable, robust, and configurable file-driven job orchestration in the cloud.

## Features
- Monitors a directory for new files using Databricks AutoLoader (Structured Streaming)
- Triggers Databricks jobs based on a mapping table
- Parameterized via Databricks widgets for flexible configuration
- Robust error handling and logging
- Modular deployment function to set up required tables
- Easily extensible for notifications, advanced mapping, and more

## Table of Contents
- [Setup](#setup)
- [Deployment](#deployment)
- [Usage](#usage)
- [Customization](#customization)
- [Troubleshooting](#troubleshooting)

## Setup

1. **Clone the repository** and upload the `Job Trigger Mech.py` script to your Databricks workspace.
2. **Configure Databricks secrets** for any credentials used in the script (e.g., `sp-id`, `sp-key`).
3. **Set up Databricks widgets** in your notebook or job to parameterize the following:
   - `monitored_directory`: Directory to monitor for new files
   - `checkpoint_directory`: Directory for streaming checkpointing
   - `workflow_mapping_table`: Name of the Delta table for directory-to-workflow mapping
   - `log_table`: Name of the Delta table for job trigger logs
   - `batch_interval`: (Optional) Streaming trigger interval
   - `cloudFiles_*`: All relevant AutoLoader options (see script for full list)

## Deployment

Before running the main streaming logic, deploy the workflow mapping table using the provided function. This ensures the required table exists with the correct schema.

**Example (in script):**
```python
deploy_framework(workflow_mapping_table)
```

This will create the table if it does not exist, with the following columns:
- `directory` (STRING, NOT NULL)
- `workflow_name` (STRING, NOT NULL)
- `active_ind` (STRING, NOT NULL)
- `last_change_user` (STRING)
- `last_updated_ts` (TIMESTAMP)

## Usage

1. **Run the script** in a Databricks notebook or as a job.
2. **Configure widgets** as needed for your environment.
3. **Populate the workflow mapping table** with directory-to-workflow mappings. Only rows with `active_ind = 'Y'` will be used.
4. **Monitor the log table** for job trigger results and troubleshooting.

## Customization
- **Add notification logic** (e.g., email, Slack) in the `process_batch` function.
- **Extend the mapping table** with additional metadata columns as needed.
- **Integrate with external monitoring** for advanced observability.
- **Modify error handling** to log to a persistent error table or external system.

## Troubleshooting
- Ensure all required widgets and secrets are set before running the script.
- Check the log table for error codes (`run_id = -1` for missing mapping, `-2` for errors).
- Use Databricks job and cluster logs for deeper debugging.

---

**Maintainer:** Jagjit Natt (HLS SSA Team)