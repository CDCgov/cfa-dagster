# Tutorial on Running Workflow Through Azure

First, build and push your Docker image by following the instructions on [creating a job to build and push image to ACR](/docs/docs/tutorial/image.md).

## Azure CAJ
To run container in Azure Container App Jobs, modify the defs object at the bottom of the `dagster_defs.py` file by uncommenting the Azure CAJ configuration.
```
defs = dg.Definitions(
    **collected_defs,
    resources={
        # This IOManager lets Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
        # an example storage account
        "azure_blob_storage": AzureBlobStorageResource(
            account_url=f"{storage_account}.blob.core.windows.net",
            credential=AzureBlobStorageDefaultCredential(),
        ),
    },
    executor=dynamic_executor(
        # default_config=default_config,
        # default_config=docker_config,
          default_config=azure_caj_config,
        # default_config=azure_batch_config,
        # alternate configs show you default values in the Launchpad on hover
        alternate_configs=[
            default_config,
            docker_config,
            azure_caj_config,
            azure_batch_config,
        ],
    ),
)
```

## Azure Batch
To run the container in Azure Batch, modify the defs object at the bottom of the `dagster_defs.py` file by uncommenting the Azure Batch configuration.
```
defs = dg.Definitions(
    **collected_defs,
    resources={
        # This IOManager lets Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
        # an example storage account
        "azure_blob_storage": AzureBlobStorageResource(
            account_url=f"{storage_account}.blob.core.windows.net",
            credential=AzureBlobStorageDefaultCredential(),
        ),
    },
    executor=dynamic_executor(
        # default_config=default_config,
        # default_config=docker_config,
        # default_config=azure_caj_config,
         default_config=azure_batch_config,
        # alternate configs show you default values in the Launchpad on hover
        alternate_configs=[
            default_config,
            docker_config,
            azure_caj_config,
            azure_batch_config,
        ],
    ),
)
```
