#!/bin/bash

MAPPER_COUNT=$1
INSTANCE_ZONE=$2

echo "**** PWD !!!! *** is ${PWD}"

for ((i=1; i<=${MAPPER_COUNT}; i++))
do
    MAPPER_INSTANCE_NAME="mapper${i}"
    gcloud compute scp --recurse ../gcp-map-reduce ${MAPPER_INSTANCE_NAME}:~ --zone=${INSTANCE_ZONE}
    gcloud compute ssh ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "sudo apt-get -qq install python3-pip && pip install -r ./gcp-map-reduce/requirements.txt"
    gcloud compute ssh ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "python3 gcp-map-reduce/scripts/mapper.py"
done