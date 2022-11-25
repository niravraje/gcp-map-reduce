import json
import socket
import pickle
import logging

SIZE = 4096

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
    client.sendall(pickle.dumps(payload))

    serialized_msg_list = []
    while True:
        packet = client.recv(SIZE)
        if not packet:
            print(f"[REDUCER - {reducer_id}] No packet received. Breaking.")
            logging.info(f"[{reducer_id}] No packet received. Breaking.")
            break
        serialized_msg_list.append(packet)
        # if len(packet) < SIZE:
        #     print(f"[REDUCER - {reducer_id}] Packet length ({len(packet)}) less than SIZE ({SIZE}). Breaking.")
        #     break
    serialized_msg = b"".join(serialized_msg_list)
    reducer_input = pickle.loads(serialized_msg)
    return reducer_input

def send_reducer_output_to_kvstore(reducer_id, reducer_output, kv_store_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("set", "reducer-output", reducer_id, reducer_output)
    client.sendall(pickle.dumps(payload))
    response = client.recv(SIZE)
    # response = pickle.loads(response)
    print(f"[REDUCER - {reducer_id}] Response for sending mapper output to KV store: {response}")
    logging.info(f"[{reducer_id}] Response for sending mapper output to KV store: {response}")

def send_ack_to_master(reducer_id, master_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(master_addr)
    payload = (reducer_id, "DONE")
    client.sendall(pickle.dumps(payload))
    response = client.recv(SIZE)
    # response = pickle.loads(response)
    print(f"[REDUCER - {reducer_id}] Response for sending ACK to master: {response}")
    logging.info(f"[{reducer_id}] Response for sending ACK to master: {response}")

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
    

