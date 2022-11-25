import json
import socket
import pickle
import logging

SIZE = 4096

def get_dataset_from_kvstore(mapper_id, kv_store_addr):
    logging.info(f"[{mapper_id}] Retrieving mapper input dataset from KV store...")

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)

    payload = ("get", "input", mapper_id)
    client.sendall(pickle.dumps(payload))

    serialized_msg_list = []
    while True:
        packet = client.recv(SIZE)
        if not packet:
            print(f"[MAPPER - {mapper_id}] No packet received. Breaking.")
            break
        serialized_msg_list.append(packet)

        # if len(packet) < SIZE:
        #     print(f"[MAPPER - {mapper_id}] Packet length ({len(packet)}) less than SIZE ({SIZE}). Breaking.")
        #     break
    
    serialized_msg = b"".join(serialized_msg_list)
    dataset = pickle.loads(serialized_msg)
    return dataset

def send_mapper_output_to_kvstore(mapper_id, mapper_output, kv_store_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("set", "mapper-output", mapper_id, mapper_output)
    client.sendall(pickle.dumps(payload))
    response = client.recv(SIZE)
    # response = pickle.loads(response)
    print(f"[MAPPER - {mapper_id}] Response for sending mapper output to KV store: {response}")
    logging.info(f"[{mapper_id}] Response for sending mapper output to KV store: {response}")

def send_ack_to_master(mapper_id, master_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(master_addr)
    payload = (mapper_id, "DONE")
    client.sendall(pickle.dumps(payload))
    response = client.recv(SIZE)
    # response = pickle.loads(response)
    print(f"[MAPPER - {mapper_id}] Response for sending ACK to master: {response}")
    logging.info(f"[{mapper_id}] Response for sending ACK to master: {response}")

def mapper_init(mapper_id, map_func, config):
    logging.basicConfig(
        filename=config["mapper_log_path"], 
        filemode='w', 
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.DEBUG
        )

    print(f"[MAPPER - {mapper_id}] {mapper_id} has started...")
    logging.info(f"{mapper_id} has started...")

    # read config params
    master_addr = (config["master_host"], config["master_port"])
    kv_store_addr = (config["kv_store_host"], config["kv_store_port"])
    
    # retrieve dataset from kv store
    dataset = get_dataset_from_kvstore(mapper_id, kv_store_addr)

    # perform map operation
    mapper_output = map_func(dataset, mapper_id)

    # send intermediate output to kvstore
    send_mapper_output_to_kvstore(mapper_id, mapper_output, kv_store_addr)

    # notify master that task is complete
    send_ack_to_master(mapper_id, master_addr)