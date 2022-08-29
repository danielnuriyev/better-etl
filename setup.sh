#!/bin/bash

deactivate
rm -rf .dagster
rm -rf .venv
rm -rf src/better_etl.egg-info

python3.10 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
python -m pip install -e ".[mysql]"

mkdir .dagster
export DAGSTER_HOME=$(pwd)/.dagster
cp dagster.yaml .dagster/dagster.yaml

# dagster-daemon run &
# dagit -w workspace.yaml
