# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "azure-identity==1.21.0",
#     "azure-keyvault-secrets",
#     "psycopg2-binary",
# ]
# ///

import os
from pathlib import Path

import psycopg2
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Authenticate using DefaultAzureCredential
credential = DefaultAzureCredential()

# Connect to the CFA-Tools Key Vault
key_vault_url = "https://CFA-Predict.vault.azure.net/"
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Fetch secrets
db_host = client.get_secret("cfa-pg-dagster-dev-host").value
db_username = client.get_secret("cfa-pg-dagster-dev-admin-username").value
db_password = client.get_secret("cfa-pg-dagster-dev-admin-password").value
existing_db_name = "postgres"

# Create a new database for the user
user_db_name = os.environ.get("USER")
if not user_db_name:
    raise ValueError("USER environment variable not set")

conn = None
try:
    conn = psycopg2.connect(
        dbname=existing_db_name,
        user=db_username,
        password=db_password,
        host=db_host,
        port="5432",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE {user_db_name} TEMPLATE postgres")
        print(f"Database '{user_db_name}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{user_db_name}' already exists.")
    finally:
        cursor.close()
except psycopg2.Error as e:
    print(f"Error connecting to or creating database: {e}")
finally:
    if conn:
        conn.close()

# create a dagster.yaml file with the new database
dagster_yaml_raw = f"""
compute_logs:
  module: dagster_azure.blob.compute_log_manager
  class: AzureBlobComputeLogManager
  config:
    storage_account: cfadagsterdev
    container: cfadagsterdev
    default_azure_credential:
    prefix: "log-files"
    local_dir: "/tmp/dagster-logs"
    upload_interval: 30
    show_url_only: false

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 25

storage:
  postgres:
    postgres_db:
      hostname: {db_host}
      username: {db_username}
      password: {db_password}
      db_name: {user_db_name}
      port: 5432

"""
# write to ~/.dagster_home/dagster.yaml
dagster_home = Path.home() / ".dagster_home"
dagster_home.mkdir(parents=True, exist_ok=True)
config_path = dagster_home / "dagster2.yaml"
config_path.write_text(dagster_yaml_raw)
print("Created ~/.dagster_home/dagster2.yaml")
