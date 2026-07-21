# Tutorial on Running Workflow Through Docker

First, build and push your Docker image by following the instructions on [creating a job to build and push image to ACR](/docs/docs/tutorial/image.md).

## Docker
To run your container in Docker,
1. Include the following configuration at the top of your `dagster_defs.py` file. Make sure that "image" matches your image built in the previous step.
```
docker_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=docker_executor.__name__,
        config={
            # specify a default image
            "image": image,
            # set env vars here
            # "env_vars": [f"DAGSTER_USER"],
            "container_kwargs": {
                "volumes": [
                    # bind the ~/.azure folder for optional cli login
                    f"/home/{user}/.azure:/root/.azure",
                    # bind current file so we don't have to rebuild
                    # the container image for workflow changes
                    f"{__file__}:{workdir}/{os.path.basename(__file__)}",
                ]
            },
        },
    )
)
```
2. Locate the `defs = dg.Definitions...` object towards the bottom of the `dagster_defs.py` file.
3. Set the `default_config` for the `dynamic_executor` to be `docker_config`.
