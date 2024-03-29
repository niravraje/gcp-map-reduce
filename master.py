import multiprocessing as mp
import json
import os
import glob
import socket
import pickle
import time
import string
import threading
import logging
from scripts.kv_store_server import start_kv_server
from scripts import mapper
from scripts import reducer
from importlib import import_module

from utils.instance_utils import *
import subprocess

# GCP modules
from googleapiclient import discovery

CONFIG_FILE_PATH = "config.json"
MESSAGE_FORMAT = "utf-8"
SIZE = 4096

def import_map_reduce_functions(config):
    # import the map/reduce functions as specified in config.json

    logging.info("Importing")

    mapper_app_module = import_module(config["mapper_function"])
    reducer_app_module = import_module(config["reducer_function"])
    if config["mapper_function"] == "scripts.wordcount_map" or config["operation_name"] == "wordcount":
        map_func = mapper_app_module.wordcount_map_init
    else:
        map_func = mapper_app_module.invertedindex_map_init
    
    if config["reducer_function"] == "scripts.wordcount_reduce" or config["operation_name"] == "wordcount":
        reduce_func = reducer_app_module.wordcount_reduce_init
    else:
        reduce_func = reducer_app_module.invertedindex_reduce_init
    return map_func, reduce_func

def cleanup_lines_list(doc_lines_list):
    # remove punctuations 
    punctuation_set = set(string.punctuation)
    for i in range(len(doc_lines_list)):
        doc_lines_list[i] = doc_lines_list[i].translate(str.maketrans('', '', string.punctuation))
    
    # strip whitespaces and newline chars
    doc_lines_list = [doc_lines_list[i].strip() for i in range(len(doc_lines_list))]
    
    # remove blank lines
    doc_lines_list = [doc_lines_list[i] for i in range(len(doc_lines_list)) if doc_lines_list[i]]
    
    # convert all words to lower case
    for i in range(len(doc_lines_list)):
            doc_lines_list[i] = doc_lines_list[i].lower()
            line_encode = doc_lines_list[i].encode("ascii", "ignore")
            doc_lines_list[i] = line_encode.decode()
    return doc_lines_list
    
def generate_dataset(raw_input_data_path):
    dataset = {}
    for filename in os.listdir(raw_input_data_path):
        filepath = os.path.join(raw_input_data_path, filename)
        with open(filepath, "r") as fp:
            doc_lines_list = fp.readlines()
        doc_lines_list = cleanup_lines_list(doc_lines_list)
        dataset[filename] = doc_lines_list
    return dataset

def load_data_in_kvstore(kv_store_addr, dataset, mapper_count):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("set", "input", dataset, mapper_count)
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")

def wait_for_mappers(master_server, config):
    count = 0
    mapper_count = config["mapper_count"]

    # Wait for explicit ACK from all mappers 
    while count < mapper_count:
        conn, client_addr = master_server.accept()
        print(f"[MASTER] Connection request accepted from mapper {client_addr}")

        serialized_msg = b""
        while True:
            packet = conn.recv(SIZE)
            serialized_msg += packet
            if b"ENDOFDATA" in packet:
                break
        serialized_msg = serialized_msg[:-9] # exclude ENDOFDATA
        payload = pickle.loads(serialized_msg)
        
        if payload[0][:6] == "mapper" and payload[1] == "DONE":
            print(f"[MASTER] {payload[0]} task completed")
            logging.info(f"{payload[0]} task completed")
            count += 1
    print(f"\n[MASTER] ACK received from all {mapper_count} mappers")
    logging.info(f"ACK received from all {mapper_count} mappers")
    print(f"\n[MASTER] --------- BARRIER (waiting for mappers to complete) ---------- \n")
    logging.info(f"BARRIER (waiting for mappers to complete)")

def wait_for_reducers(master_server, config):
    count = 0
    reducer_count = config["reducer_count"]

    # Wait for explicit ACK from all reducers
    while count < reducer_count:
        conn, client_addr = master_server.accept()
        print(f"[MASTER] Connection request accepted from reducer {client_addr}")

        serialized_msg = b""
        while True:
            packet = conn.recv(SIZE)
            serialized_msg += packet
            if b"ENDOFDATA" in packet:
                break
        serialized_msg = serialized_msg[:-9] # exclude ENDOFDATA
        payload = pickle.loads(serialized_msg)

        if payload[0][:7] == "reducer" and payload[1] == "DONE":
            print(f"[MASTER] {payload[0]} task completed")
            logging.info(f"{payload[0]} task completed")
            count += 1

    print(f"\n[MASTER] ACK received from all {reducer_count} reducers")
    logging.info(f"ACK received from all {reducer_count} reducers")

def cleanup_kvstore(kv_store_addr):
    print(f"**** cleanup *** {kv_store_addr}")
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("cleanup", "all")
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")

def combine_reducer_output(kv_store_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("combine", "final-output")
    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")

def launch_kv_store(compute, config):
    project = config["project_id"]
    zone = config["zone"]
    kv_store_instance_name = config["kv_store_instance_name"]

    operation = create_instance(compute=compute, project=project, zone=zone, name=kv_store_instance_name)
    wait_for_operation(compute, project, zone, operation['name'])
    kv_store_instance_obj = get_instance_obj(compute, project, zone, kv_store_instance_name)
    kv_internal_ip = get_instance_internal_ip(kv_store_instance_obj)
    kv_external_ip = get_instance_external_ip(kv_store_instance_obj)
    print(f"KV Store Created.")
    print(f"KV Store Internal IP: {kv_internal_ip}")
    print(f"KV Store External IP: {kv_external_ip}")
    
    time.sleep(5)
    subprocess.call(["/bin/bash", "./shell-scripts/kv_store_init.sh", config["kv_store_instance_name"], zone])
    
    config["kv_store_host"] = kv_internal_ip

    return kv_store_instance_obj

def launch_mappers(compute, config):
    project = config["project_id"]
    zone = config["zone"]
    mapper_count = config["mapper_count"]
    mapper_obj_table = {}
    operations = []

    for i in range(1, mapper_count+1):
        mapper_instance_name = f"mapper{i}"
        operation = create_instance(compute=compute, project=project, zone=zone, name=mapper_instance_name)
        operations.append(operation)
    
    for oper in operations:
        wait_for_operation(compute, project, zone, oper['name'])
        mapper_instance_obj = get_instance_obj(compute, project, zone, mapper_instance_name)
        mapper_obj_table[mapper_instance_name] = mapper_instance_obj

    time.sleep(max(5 * mapper_count, 10))
    subprocess.call(["/bin/bash", "./shell-scripts/mapper_init.sh", str(mapper_count), zone])
    return mapper_obj_table

def launch_reducers(compute, config):
    project = config["project_id"]
    zone = config["zone"]
    reducer_count = config["reducer_count"]
    reducer_obj_table = {}
    operations = []

    for i in range(1, reducer_count+1):
        reducer_instance_name = f"reducer{i}"
        operation = create_instance(compute=compute, project=project, zone=zone, name=reducer_instance_name)
        operations.append(operation)
    
    for oper in operations:
        wait_for_operation(compute, project, zone, oper['name'])
        reducer_instance_obj = get_instance_obj(compute, project, zone, reducer_instance_name)
        reducer_obj_table[reducer_instance_name] = reducer_instance_obj

    subprocess.call(["/bin/bash", "./shell-scripts/reducer_init.sh", str(reducer_count), zone])
    return reducer_obj_table

def update_config_file(config):
    print("[MASTER] Updating config file...")
    with open(CONFIG_FILE_PATH, "w") as fp:
        json.dump(config, fp, indent=4)

def master_init():
    
    # read config parameters
    with open(CONFIG_FILE_PATH, "r") as fp:
        config = json.load(fp)

    # initialize logging configurations
    logging.basicConfig(
        filename=config["master_log_path"], 
        filemode='w', 
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.DEBUG
        )

    config["master_host"] = socket.gethostbyname(socket.gethostname())
    master_addr = (config["master_host"], config["master_port"])
    operation_name = config["operation_name"]

    # optional: ensuring mapper and reducer functions are correctly used based \
    # on operation_name in case there are any typos in config.json function names
    # if config["ignore_function_names"] is false, the values from config.json are used
    if config["ignore_function_names"] == "true":
        if operation_name == "invertedindex":
            config["mapper_function"] = "invertedindex_map"
            config["reducer_function"] = "invertedindex_reduce"
        else:
            config["mapper_function"] = "wordcount_map"
            config["reducer_function"] = "wordcount_reduce"

    # Open master server socket & start listening for connections
    print(f"[MASTER] Master process has started for {operation_name} operation...")
    logging.info(f"Master process has started for {operation_name} operation...")
    # Create master socket
    master_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_server.bind(master_addr)
    master_server.listen()
    print(f"[MASTER] Server listening for connections at address {master_addr}...")
    logging.info(f"Server listening for connections at address {master_addr}...")

    subprocess.call(["/bin/bash", "./shell-scripts/master-init.sh"])

    compute = discovery.build('compute', 'v1')
    
    kv_store_instance_obj = launch_kv_store(compute, config)

    # Update config file with new IPs of master, kv_store_server & any other changes
    update_config_file(config)
    
    print("*** config at this point is\n", config)

    # cleanup kv store
    print(f"[MASTER] Cleaning up KV Store...")
    logging.info(f"Cleaning up KV Store...")
    kv_store_addr = (config["kv_store_host"], config["kv_store_port"])
    cleanup_kvstore(kv_store_addr)
    time.sleep(2)
    
    # generate dataset dictionary from raw-dataset
    print(f"[MASTER] Partitioning raw dataset as per number of mappers...")
    logging.info(f"Partitioning raw dataset as per number of mappers...")
    dataset = generate_dataset(config["raw_input_data_path"])

    # load dataset in "input" kv-store
    print(f"[MASTER] Loading partitioned mapper-input files into KV Store...")
    logging.info(f"Loading partitioned mapper-input files into KV Store...")
    load_data_in_kvstore(kv_store_addr, dataset, config["mapper_count"])
    time.sleep(1)

    mapper_obj_table = launch_mappers(compute, config)

    # Barrier: Wait for all mappers to complete
    wait_for_mappers(master_server, config)

    mapper_count = config["mapper_count"]
    print(f"\n[MASTER] All {mapper_count} mapper tasks are complete...\n")
    logging.info(f"All {mapper_count} mapper tasks are complete...")

    reducer_obj_table = launch_reducers(compute, config)

    # Wait for all reducers to complete: only applicable if single output file is desired
    wait_for_reducers(master_server, config)

    print(f"[MASTER] Generating final output file & writing to {config['final_output_path']}...")
    logging.info(f"Generating final output file & writing to {config['final_output_path']}...")
    # Combine reducers' output into a single file
    combine_reducer_output(kv_store_addr)

    # cleanup (delete all mapper & reducer VMs)
    subprocess.call(["/bin/bash", "./shell-scripts/cleanup.sh", str(config["mapper_count"]), str(config["reducer_count"]), config["zone"]])

if __name__ == "__main__":
    master_init()
    
    







    

