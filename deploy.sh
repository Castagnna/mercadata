#!/usr/bin/env bash

DIR_BASE_LOCAL=$(pwd)
GCS_BUCKET="gs://mercafacil"

echo "Building python egg"
pip install -r requirements.txt
python3 setup.py bdist_egg clean --all

echo "Coping egg to bucket"
gsutil -m cp $DIR_BASE_LOCAL/dist/mercadata-0.0.1-py3.9.egg "$GCS_BUCKET/eggs"

echo "Coping launcher to bucket"
gsutil -m cp $DIR_BASE_LOCAL/etl/laucher.py "$GCS_BUCKET/eggs"
