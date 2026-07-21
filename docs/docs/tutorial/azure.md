# Tutorial on Running Workflow Through Azure

First, build and push your Docker image by following the instructions on [creating a job to build and push image to ACR](image.md).

## Azure CAJ
To run the container in Azure CAJ,

1. Include the following configuration at the top of your `dagster_defs.py` file. Modify the "image" to match the image built in the previous step.
```
azure_caj_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_container_app_job_executor.__name__,
        config={
            "container_app_job_name": "cfa-dagster",
            # specify a default image
            "image": image,
            # set env vars here
            # "env_vars": [f"DAGSTER_USER"],
        },
    )
)
```
2. Locate the `defs = dg.Definitions...` object towards the bottom of the `dagster_defs.py` file.
3. Set the `default_config` for the `dynamic_executor` to be `azure_caj_config`.

## Azure Batch
To run the container in Azure Batch,

1. Include the following configuration at the top of your `dagster_defs.py` file. Modify the "pool_name", "working_dir", and "container_kwargs" as needed. Make sure that "image" matches your image built in the previous step.
```
azure_batch_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=azure_batch_executor.__name__,
        config={
            # change the pool_name to your existing pool name
            "pool_name": "cfa-dagster",
            # specify a default image
            "image": image,
            # set env vars here
            "env_vars": ["CFA_DAGSTER_LOG_LEVEL=debug"],
            "container_kwargs": {
                # set the working directory to match your Dockerfile
                # required for Azure Batch
                "working_dir": workdir,
                # mount config if your existing Batch pool already has Blob mounts
                # "volumes": [
                #     "nssp-etl:nssp-etl",
                # ]
            },
        },
    ),
)
```
2. Locate the `defs = dg.Definitions...` object towards the bottom of the `dagster_defs.py` file.
3. Set the `default_config` for the `dynamic_executor` to be `azure_batch_config`.
