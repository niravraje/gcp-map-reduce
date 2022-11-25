import os
import time


def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None

def create_instance(compute, project, zone, name):
    image_response = compute.images().getFromFamily(
        project='debian-cloud', family='debian-11').execute()
    source_disk_image = image_response['selfLink']

    machine_type = f"zones/{zone}/machineTypes/g1-small"

    config = {
        'name': name,
        'machineType': machine_type,
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }]
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()


def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)

def get_instance_obj(compute, project, zone, instance_name):
    instances = list_instances(compute, project, zone)
    for instance_obj in instances:
        if instance_obj["name"] == instance_name:
            return instance_obj

def get_instance_internal_ip(instance_obj):
    return instance_obj["networkInterfaces"][0]["networkIP"]

def get_instance_external_ip(instance_obj):
    return instance_obj["networkInterfaces"][0]["accessConfigs"][0]["natIP"]
