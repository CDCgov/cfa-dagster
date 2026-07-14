# Tutorial on Running Workflow Through Docker

## General

1. Login to Azure Container Registry and pull the latest container image
```
REGISTRY=cfaprdbatchcr.azurecr.io/
IMAGE_NAME=<Your GitHub repository name>
TAG=<Your GitHub repo branch name within the repo specified on the previous line>

az acr login --name 'cfaprdbatchcr'
docker pull $(REGISTRY)$(IMAGE_NAME):$(TAG)
```
2. Build the Docker image with given tag
```
docker build -t $(REGISTRY)$(IMAGE_NAME):$(TAG) \
		--build-arg TAG=$(TAG) -f Dockerfile .
```
3. Push the tagged image to the container registry
```
docker push $(REGISTRY)$(IMAGE_NAME):$(TAG)
```

## Docker
1. Modify the defs object at the bottom of the `dagster_defs.py` file by uncommenting the docker configuration.
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
          default_config=docker_config,
        # default_config=azure_caj_config,
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
