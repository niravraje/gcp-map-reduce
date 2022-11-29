#!/bin/bash

REDUCER_COUNT=$1
INSTANCE_ZONE=$2

echo "**** PWD !!!! *** is ${PWD}"
echo "Setting up ${REDUCER_COUNT} reducers. Sleeping 30 seconds to allow the VMs to accept SSH on port 22"

sleep 30
for ((i=1; i<=${REDUCER_COUNT}; i++))
do
    REDUCER_INSTANCE_NAME="reducer${i}"
    echo "Initializing setup for ${REDUCER_INSTANCE_NAME}"
    gcloud compute ssh ${REDUCER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "echo start"
    sleep 5
    gcloud compute ssh ${REDUCER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "sudo apt-get -qq install python3-pip"
    gcloud compute scp --recurse ../gcp-map-reduce ${REDUCER_INSTANCE_NAME}:~ --zone=${INSTANCE_ZONE}
    gcloud compute ssh ${REDUCER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "pip install -r ./gcp-map-reduce/requirements.txt"
    gcloud compute ssh ${REDUCER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "python3 gcp-map-reduce/scripts/reducer.py"
done