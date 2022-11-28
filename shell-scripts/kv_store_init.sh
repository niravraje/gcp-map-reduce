#!/bin/bash

set -x

KV_STORE_INSTANCE_NAME=$1
INSTANCE_ZONE=$2

echo "Executing kv_store_init.sh from PWD=${PWD}"

gcloud compute scp --recurse ../gcp-map-reduce ${KV_STORE_INSTANCE_NAME}:~ --zone=${INSTANCE_ZONE}
gcloud compute ssh ${KV_STORE_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "sudo apt-get -qq install python3-pip && pip install -r ./gcp-map-reduce/requirements.txt" --quiet

gcloud compute ssh ${KV_STORE_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "python3 gcp-map-reduce/scripts/kv_store_server.py" &

sleep 8

echo "KV store launch tasks complete"