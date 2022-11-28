#!/bin/bash

# set -x

source ./infra_config.sh

for ((i=1; i<=${MAPPER_COUNT}; i++))
do
    MAPPER_INSTANCE_NAME="mapper${i}"
    echo "Launching ${MAPPER_INSTANCE_NAME}"
    gcloud compute instances create ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} --machine-type=${MACHINE_TYPE} || true
done

sleep 10

for ((i=1; i<=${MAPPER_COUNT}; i++))
do
    MAPPER_INSTANCE_NAME="mapper${i}"
    gcloud compute scp --recurse ../gcp-map-reduce ${MAPPER_INSTANCE_NAME}:~ --zone=${INSTANCE_ZONE}
    gcloud compute ssh ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} -- "sudo apt-get -qq install python3-pip && pip install -r ./gcp-map-reduce/requirements.txt"
done



echo "all done"