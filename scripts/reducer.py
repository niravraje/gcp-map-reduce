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

def group_by_alphabet(reducer_count):
    letters = "abcdefghijklmnopqrstuvwxyz"
    groups = [""] * reducer_count
    i = 0
    for c in letters:
        groups[i % reducer_count] += c
        i += 1
    return groups

def get_reducer_input_from_kvstore(kv_store_addr, group, reducer_id):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)

    payload = ("get", "mapper-output", group, reducer_id)
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")

    serialized_msg = b""
    while True:
        packet = client.recv(SIZE)
        serialized_msg += packet
        if b"ENDOFDATA" in packet:
            logging.info(f"ENDOFDATA received. Breaking...")
            break

    serialized_msg = serialized_msg[:-9] # exclude ENDOFDATA
    reducer_input = pickle.loads(serialized_msg)
    return reducer_input

def send_reducer_output_to_kvstore(reducer_id, reducer_output, kv_store_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("set", "reducer-output", reducer_id, reducer_output)
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")
    response = client.recv(SIZE)
    # response = pickle.loads(response)
    print(f"[REDUCER - {reducer_id}] Response for sending mapper output to KV store: {response}")
    logging.info(f"[{reducer_id}] Response for sending mapper output to KV store: {response}")

def send_ack_to_master(reducer_id, master_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(master_addr)
    payload = (reducer_id, "DONE")
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")

def reducer_init(reducer_id, reduce_func, config):

    # initialize logging configurations
    logging.basicConfig(
        filename=config["reducer_log_path"], 
        filemode='w', 
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.DEBUG
        )

    print(f"{reducer_id} has started...")
    logging.info(f"{reducer_id} has started...")

    # read config params
    master_addr = (config["master_host"], config["master_port"])
    kv_store_addr = (config["kv_store_host"], config["kv_store_port"])

    # groupby alphabet and reducer count
    groups = group_by_alphabet(config["reducer_count"])
    reducer_num = int(reducer_id[7:])
    curr_reducer_group = groups[reducer_num-1]
    print(f"[REDUCER - {reducer_id}] group string: {curr_reducer_group}")
    logging.info(f"[{reducer_id}] group string: {curr_reducer_group}")
    
    # retrieve intermediate output as per groupby strategy from mappers
    reducer_input = get_reducer_input_from_kvstore(kv_store_addr, curr_reducer_group, reducer_id)
    
    # perform reduce operation
    reducer_output = reduce_func(reducer_input, reducer_id)

    # send reducer output to kvstore
    send_reducer_output_to_kvstore(reducer_id, reducer_output, kv_store_addr)

    # notify master that task is complete
    send_ack_to_master(reducer_id, master_addr)

if __name__ == "__main__":
    reducer_id = socket.gethostname()
    with open("./gcp-map-reduce/config.json", "r") as fp:
        config = json.load(fp)
    _, reduce_func = import_map_reduce_functions(config)
    reducer_init(reducer_id, reduce_func, config)
    

