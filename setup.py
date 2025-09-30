# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "azure-identity==1.21.0",
#     "azure-keyvault-secrets",
# ]
# ///

from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Authenticate using DefaultAzureCredential
credential = DefaultAzureCredential()

# Connect to the CFA-Tools Key Vault
key_vault_url = "https://CFA-Predict.vault.azure.net/"
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Fetch secrets
db_host = client.get_secret("cfa-pg-dagster-dev-host").value
db_username = client.get_secret("cfa-pg-dagster-dev-admin-username").value
db_password = client.get_secret("cfa-pg-dagster-dev-admin-password").value
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
  class: DefaultRunCoordinator

storage:
  postgres:
    postgres_db:
      hostname: {db_host}
      username: {db_username}
      password: {db_password}
      db_name: "postgres"
      port: 5432

"""
# write to ~/.dagster_home/dagster.yaml
dagster_home = Path.home() / ".dagster_home"
dagster_home.mkdir(parents=True, exist_ok=True)
config_path = dagster_home / "dagster.yaml"
config_path.write_text(dagster_yaml_raw)
print("Created ~/.dagster_home/dagster.yaml")
