#!/bin/bash

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
python -m pip install -e ".[mysql]"
