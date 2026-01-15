# cfa-dagster

## Overview

This repo serves as the base for CFA's Dagster software including custom executors, IO Managers, Infrastructure as Code, etc.

## What is Dagster?

[Dagster](https://docs.dagster.io/) is an orchestration platform that provides observability into your workflows as they are transparently executed in either local or cloud environments. 

Dagster unifies logic that is often distributed between GitHub Actions workflows, Makefiles, Bash and Python Scripts, and configured in the Azure Portal.

## Why use Dagster?

Dagster allows you to run the same code on your machine, in Docker, on Azure Batch, or on Azure Container App Jobs with a simple configuration change

Dagster provides a clear user interface to expose events, metadata, stdout, and stdin logs for your workload regardless of the compute environment.

Dagster has built-in schedulers and event-based triggers to run your workload when you want.


## Getting started

- To get started with local Dagster development, clone this repo and check out the [examples](examples/)

- To use this repo as a python library, add the dependency to your `pyproject.toml` or inline PEP 723 script comment:
```toml
 dependencies = [
    "cfa-dagster @ git+https://github.com/cdcgov/cfa-dagster.git",
 ]

```

## Moving local workflow to production
If you would like to schedule your workflow to run on a schedule or triggered based on other workflows, you can move your workflows to the production server (http://dagster.apps.edav.ext.cdc.gov) with the following:

- Build your workflow into a Docker image and push to a container registry. Your workflow file must be named `dagster_defs.py` and in the `WORKDIR` of your image. Be sure to `uv sync` your workflow dependencies and add them to the `PATH`. See the examples for more info.

- To create a workflow or update an existing one, run `uv run https://raw.githubusercontent.com/CDCgov/cfa-dagster/refs/heads/main/scripts/update_code_location.py --registry_image <your_registry_image>`. Your registry image can be `cfaprdbatchcr.azurecr.io/{image}:{tag}` or `ghcr.io/cdcgov/{image}:{tag}`. Images from cdcent cannot be used due to privacy and credential restrictions.

New workflows will be created as 'code locations' on the server named to match your registry image with underscored replaced by hyphens e.g.

- registering the image `cfaprdbatchcr.azurecr.io/cfa_dagster:latest` will result in a code location named `cfa-dagster`

This means you *cannot* register the same image with different tags e.g. `cfa_dagster:prod` & `cfa_dagster:feature`

## Infrastructure
For information about the infrastructure, see the [infra](infra/) folder

## Future Development

- A Blob File IO Manager to pass data between assets in a file format

## Project admins

- Giovanni Rella <ap82@cdc.gov> (CDC/OD/ORR/CFA)

## Disclaimers

### General Disclaimer

This repository was created for use by CDC programs to collaborate on public health related projects in support of the [CDC mission](https://www.cdc.gov/about/organization/mission.htm). GitHub is not hosted by the CDC, but is a third party website used by CDC and its partners to share information and collaborate on software. CDC use of GitHub does not imply an endorsement of any one particular service, product, or enterprise.

### Public Domain Standard Notice

This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
The repository utilizes code licensed under the terms of the Apache Software
License and therefore is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](DISCLAIMER.md)
and [Code of Conduct](code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice
This repository is not a source of government records, but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).

## Additional Standard Notices
Please refer to [CDC's Template Repository](https://github.com/CDCgov/template) for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/main/CONTRIBUTING.md), [public domain notices and disclaimers](https://github.com/CDCgov/template/blob/main/DISCLAIMER.md), and [code of conduct](https://github.com/CDCgov/template/blob/main/code-of-conduct.md).
