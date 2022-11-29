#!/bin/bash

MAPPER_COUNT=$1
INSTANCE_ZONE=$2

echo "**** PWD !!!! *** is ${PWD}"
echo "Setting up ${MAPPER_COUNT} mappers. Sleeping 30 seconds to allow the VMs to accept SSH on port 22"

sleep 30
for ((i=1; i<=${MAPPER_COUNT}; i++))
do
    MAPPER_INSTANCE_NAME="mapper${i}"
    echo "Initializing setup for ${MAPPER_INSTANCE_NAME}"
    gcloud compute ssh ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "echo start"
    sleep 5
    gcloud compute ssh ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "sudo apt-get -qq install python3-pip"
    gcloud compute scp --recurse ../gcp-map-reduce ${MAPPER_INSTANCE_NAME}:~ --zone=${INSTANCE_ZONE}
    gcloud compute ssh ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "pip install -r ./gcp-map-reduce/requirements.txt"
    gcloud compute ssh ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "python3 gcp-map-reduce/scripts/mapper.py" &
done