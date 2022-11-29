import json
import socket
import pickle
import logging
from importlib import import_module

SIZE = 4096

def import_map_reduce_functions(config):
    # import the map/reduce functions as specified in config.json

    logging.info("Importing")

    mapper_app_module = import_module(config["mapper_function"])
    reducer_app_module = import_module(config["reducer_function"])
    if config["mapper_function"] == "wordcount_map" or config["operation_name"] == "wordcount":
        map_func = mapper_app_module.wordcount_map_init
    else:
        map_func = mapper_app_module.invertedindex_map_init
    
    if config["reducer_function"] == "wordcount_reduce" or config["operation_name"] == "wordcount":
        reduce_func = reducer_app_module.wordcount_reduce_init
    else:
        reduce_func = reducer_app_module.invertedindex_reduce_init
    return map_func, reduce_func

def get_dataset_from_kvstore(mapper_id, kv_store_addr):
    logging.info(f"[{mapper_id}] Retrieving mapper input dataset from KV store...")

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)

    payload = ("get", "input", mapper_id)
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")

    serialized_msg = b""
    while True:
        packet = client.recv(SIZE)
        serialized_msg += packet
        if b"ENDOFDATA" in packet:
            logging.info(f"ENDOFDATA received. Breaking...")
            break

    serialized_msg = serialized_msg[:-9] # exclude ENDOFDATA
    dataset = pickle.loads(serialized_msg)
    return dataset

def send_mapper_output_to_kvstore(mapper_id, mapper_output, kv_store_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("set", "mapper-output", mapper_id, mapper_output)
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")
    response = client.recv(SIZE)
    # response = pickle.loads(response)
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

if __name__ == "__main__":
    mapper_id = socket.gethostname()
    with open("./gcp-map-reduce/config.json", "r") as fp:
        config = json.load(fp)
    map_func, _ = import_map_reduce_functions(config)
    mapper_init(mapper_id, map_func, config)