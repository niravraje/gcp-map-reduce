
import os
import time

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

# compute = discovery.build('compute', 'v1', credentials=credentials)
# project = "nirav-raje-fall2022"
# zone = "europe-west2-a"
# name = "test-instance-from-py"

# operation = create_instance(compute, project, zone, name)
# wait_for_operation(compute, project, zone, operation)
# print("done done done")