#!/bin/bash

set -x

# Create the default network
gcloud compute networks create default || true

# Create firewall rules to allow traffic to instances
gcloud compute firewall-rules create default-rule-allow-internal --network default --allow tcp,udp,icmp --source-ranges 0.0.0.0/0  || true
gcloud compute firewall-rules create default-rule-allow-tcp22-tcp3389-icmp --network default --allow tcp:22,tcp:3389,icmp || true
gcloud compute firewall-rules create allow-ssh-ingress-from-iap --direction=INGRESS --action=allow --rules=tcp:22 --source-ranges=35.235.240.0/20