# Installation

* **Python:** Dagster supports Python 3.10 - 3.13 (3.12 recommended).
* **Package manager:** To manage the python packages, we recommend [uv](https://docs.astral.sh/uv/) which Dagster uses internally.
* **Git:** Refer to the [Git documentation](https://github.com/git-guides/install-git) if you don’t have this installed.

## Install Dagster
1. Activate your Python [virtual environment]( https://docs.python.org/3/library/venv.html)  
2. [Install Dagster with uv:]( https://docs.dagster.io/getting-started/installation#installation-requirements-for-manually-creating-or-updating-a-project)  
`uv add dagster dagster-webserver dagster-dg-cli`.  
Or install with pip:  
`pip install dagster dagster-webserver dagster-dg-cli`.  
3. Clone the [`cfa-dagster` repository](https://github.com/CDCgov/cfa-dagster/tree/main):  
`git clone https://github.com/CDCgov/cfa-dagster.git`  
Or  
`gh repo clone cdcgov/cfa-dagster` 
