#!/bin/bash

set -x

MAPPER_COUNT=$1
REDUCER_COUNT=$2
INSTANCE_ZONE=$3

for ((i=1; i<=${MAPPER_COUNT}; i++))
do
    MAPPER_INSTANCE_NAME="mapper${i}"
    echo "Deleting ${MAPPER_INSTANCE_NAME} VM"
    gcloud compute instances delete ${MAPPER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} --delete-disks=all --quiet &
done

for ((i=1; i<=${REDUCER_COUNT}; i++))
do
    REDUCER_INSTANCE_NAME="reducer${i}"
    echo "Deleting ${REDUCER_INSTANCE_NAME} VM"
    gcloud compute instances delete ${REDUCER_INSTANCE_NAME} --zone=${INSTANCE_ZONE} --delete-disks=all --quiet &
done
