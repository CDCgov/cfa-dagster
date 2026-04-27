from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dagster import ConfigurableResource

KEY_VAULT_URL_CFA_PREDICT = "https://CFA-Predict.vault.azure.net/"


class AzureKeyVaultResource(ConfigurableResource):
    vault_url: str = KEY_VAULT_URL_CFA_PREDICT

    def get_client(self) -> SecretClient:
        credential = DefaultAzureCredential()
        return SecretClient(vault_url=self.vault_url, credential=credential)


def get_secret(secret_name: str) -> str:
    credential = DefaultAzureCredential()
    client = SecretClient(
        vault_url=KEY_VAULT_URL_CFA_PREDICT, credential=credential
    )

    return client.get_secret(secret_name).value
