# Master Data Quality Platform — MVP Submission Guide
A two-module Streamlit application for master data rule discovery, human
approval, and automated 7-day data validation, backed by Databricks and
a local Ollama LLM.

## Prerequisites

  - Python 3.10+
  - A Databricks workspace with SQL Warehouse access (host, HTTP path, personal access token)
  - Ollama installed locally 

## Step 1 — Install Ollama and pull the model

1. Download and install Ollama for your OS from https://ollama.com/download
2. Verify the install:
  ollama --version
Verfiy in your browser - `http://localhost:11434`

## Step 2 — Databricks setup

    1. Log in to your Databricks workspace.
    2. Confirm you have a running SQL Warehouse. Note down:
    - Server hostname (e.g. `dbc-xxxxxxx-xxxx.cloud.databricks.com`)
    - HTTP path (e.g. `/sql/1.0/warehouses/xxxxxxxxxxxxxxxx`)
    - A personal access token (User Settings → Developer → Access Tokens → Generate new token)
    3. Create a catalog and a schema in your databricks
    4. Open `setup_databricks.py`, `agent_rule_discovery.py`, `master_data_rule_approval_app.py`,
    and `rule_engine.py`, and update the following constants in each file with your own values:
    ```python
    DATABRICKS_HOST  = "your-workspace-hostname"
    DATABRICKS_PATH  = "/sql/1.0/warehouses/your-warehouse-id"
    DATABRICKS_TOKEN = "your-personal-access-token"
    CATALOG          = "your-catalog-name"
    SCHEMA           = "your-schema-name"
    ```
    5. No manual file upload is required — `setup_databricks.py` creates the schema, tables,
    and generates + uploads 2,000 synthetic rows per table directly via the Databricks API.

## Step 3 — Install Python dependencies
  pip install -r requirements.txt

## Step 4 — Upload the synthetic data into Databricks(run once)
  python setup_databricks.py

## Step 5 — Run the AI Discovery Agent
  python agent_rule_discovery.py

## Step 6 — Launch the streamlit
  streamlit run master_data_rule_approval_app.py

## You can see 2 tabs here,
### Tab 1: Master Data - Rule approval
shows the rules identified by the agent via pattern recognition and awaits in the list for the human approval. It also has a field called "suggest a rule" 
which enables the user to type in a rule manually, the agent checks it against the databricks and shows the confidence level of that rule and asks for 
a confirmation to be added to the list(awaiting approval).

### Tab 2: Data Validation dashboard
Approved rules from the previous tab acts as an input in this field, and via an automation these rules runs everyday 6.00AM and the report is sent automatically
to be relevant team. Rules can also be triggered to run manually in this tab. It also displays the flag trend and the audit log history of the rules that are run.


