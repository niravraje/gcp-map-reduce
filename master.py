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

from utils.instance_utils import create_instance

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
    client.sendall(pickle.dumps(payload))

def start_mappers(config, map_func):

    # delete any previous intermediate output files
    folder_files = glob.glob("./kv-data-store/mapper-output/*")
    for f in folder_files:
        os.remove(f)

    mapper_ids = ["mapper" + str(i+1) for i in range(config["mapper_count"])]
    print("[MASTER] Mapper IDs: ", mapper_ids)
    logging.info(f"Mapper IDs: {mapper_ids}")

    mapper_process_list = []

    for mapper_id in mapper_ids:
        mapper_process = mp.Process(target=mapper.mapper_init, args=(mapper_id, map_func, config,))
        mapper_process.start()
        mapper_process_list.append(mapper_process)
    
    return mapper_process_list

def wait_for_mappers(master_server, mapper_process_list, config):
    count = 0
    mapper_count = len(mapper_process_list)

    # Wait for explicit ACK from all mappers 
    while count < mapper_count:
        conn, client_addr = master_server.accept()
        print(f"[MASTER] Connection request accepted from mapper {client_addr}")
        packet = conn.recv(SIZE)
        payload = pickle.loads(packet)
        if payload[0][:6] == "mapper" and payload[1] == "DONE":
            print(f"[MASTER] {payload[0]} task completed")
            logging.info(f"{payload[0]} task completed")
            count += 1
    print(f"\n[MASTER] ACK received from all {mapper_count} mappers")
    logging.info(f"ACK received from all {mapper_count} mappers")
    print(f"\n[MASTER] --------- BARRIER (waiting for mappers to complete) ---------- \n")
    logging.info(f"BARRIER (waiting for mappers to complete)")
    
    # ensure KV store has created all mapper output files
    # while len(glob.glob(config["mapper_output_path"])) < config["mapper_count"]:
    #     pass

    # print(f"\n[MASTER] Waiting for mappers to join...")
    # # Hard barrier: Master will block till all mapper processes have joined
    # for mapper_process in mapper_process_list:
    #     mapper_process.join()
    # print(f"\n[MASTER] All mappers have joined.")



def start_reducers(config, reduce_func):
    category_path = config["reducer_output_path"]
    # delete any previous reducer output files
    folder_files = glob.glob(os.path.join(category_path, "*"))
    for f in folder_files:
        os.remove(f)

    reducer_ids = ["reducer" + str(i+1) for i in range(config["reducer_count"])]
    print(f"[MASTER] Reducer IDs: {reducer_ids}")
    logging.info(f"Reducer IDs: {reducer_ids}")

    reducer_process_list = []
    for reducer_id in reducer_ids:
        reducer_process = mp.Process(target=reducer.reducer_init, args=(reducer_id, reduce_func, config,))
        reducer_process.start()
        reducer_process_list.append(reducer_process)
    
    return reducer_process_list

def wait_for_reducers(master_server, reducer_process_list, config):
    count = 0
    reducer_count = len(reducer_process_list)

    # Wait for explicit ACK from all reducers
    while count < reducer_count:
        conn, client_addr = master_server.accept()
        print(f"[MASTER] Connection request accepted from reducer {client_addr}")
        packet = conn.recv(SIZE)
        payload = pickle.loads(packet)
        if payload[0][:7] == "reducer" and payload[1] == "DONE":
            print(f"[MASTER] {payload[0]} task completed")
            logging.info(f"{payload[0]} task completed")
            count += 1

    print(f"\n[MASTER] ACK received from all {reducer_count} reducers")
    logging.info(f"ACK received from all {reducer_count} reducers")

    # ensure KV store has created all reducer output files
    # while len(glob.glob(config["reducer_output_path"])) < config["reducer_count"]:
    #     pass

    # Hard barrier: To run final combiner function, master will block till all reducers are done
    # print(f"\n[MASTER] Waiting for reducers to join...")
    # for reducer_process in reducer_process_list:
    #     reducer_process.join()
    # print(f"\n[MASTER] All reducers have joined.")

def cleanup_kvstore(kv_store_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("cleanup", "all")
    client.sendall(pickle.dumps(payload))
    response = client.recv(SIZE)

def combine_reducer_output(kv_store_addr):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)
    payload = ("combine", "final-output")
    client.sendall(pickle.dumps(payload))
    response = client.recv(SIZE)

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

    host = socket.gethostbyname(socket.gethostname())
    raw_input_data_path = config["raw_input_data_path"]
    mapper_count = config["mapper_count"]
    reducer_count = config["reducer_count"]
    master_addr = (host, config["master_port"])
    kv_store_addr = (config["kv_store_host"], config["kv_store_port"])
    operation_name = config["operation_name"]
    project = config["project_id"]
    zone = config["zone"]
    kv_store_instance_name = config["kv_store_instance_name"]

    compute = discovery.build('compute', 'v1')

    # Create KV-Store instance
    create_instance(compute, project, zone, kv_store_instance_name)

    # # optional: ensuring mapper and reducer functions are correctly used based \
    # # on operation_name in case there are any typos in config.json function names
    # # if config["ignore_function_names"] is false, the values from config.json are used
    # if config["ignore_function_names"] == "true":
    #     if operation_name == "invertedindex":
    #         config["mapper_function"] = "scripts.invertedindex_map"
    #         config["reducer_function"] = "scripts.invertedindex_reduce"
    #     else:
    #         config["mapper_function"] = "scripts.wordcount_map"
    #         config["reducer_function"] = "scripts.wordcount_reduce"

    # # get the application (wordcount/invertedindex) functions for map & reduce 
    # map_func, reduce_func = import_map_reduce_functions(config)

    # print(f"[MASTER] Master process has started for {operation_name} operation...")
    # logging.info(f"Master process has started for {operation_name} operation...")
    # # Create master socket
    # master_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # master_server.bind(master_addr)
    # master_server.listen()
    # print(f"[MASTER] Server listening for connections at address {master_addr}...")
    # logging.info(f"Server listening for connections at address {master_addr}...")

    # # Start KV store
    # kv_store_process = mp.Process(target=start_kv_server, args=(config,))
    # kv_store_process.start()
    # time.sleep(3)

    # # cleanup kv store
    # print(f"[MASTER] Cleaning up KV Store...")
    # logging.info(f"Cleaning up KV Store...")
    # cleanup_kvstore(kv_store_addr)
    # time.sleep(2)

    # # generate dataset dictionary from raw-dataset
    # print(f"[MASTER] Partitioning raw dataset as per number of mappers...")
    # logging.info(f"Partitioning raw dataset as per number of mappers...")
    # dataset = generate_dataset(raw_input_data_path)

    # # load dataset in "input" kv-store
    # print(f"[MASTER] Loading partitioned mapper-input files into KV Store...")
    # logging.info(f"Loading partitioned mapper-input files into KV Store...")
    # load_data_in_kvstore(kv_store_addr, dataset, mapper_count)
    # time.sleep(1)

    # # Start mappers
    # print(f"[MASTER] Starting {mapper_count} Mappers...")
    # logging.info(f"Starting {mapper_count} Mappers...")
    # mapper_process_list = start_mappers(config, map_func)

    # # Barrier: Wait for all mappers to complete
    # wait_for_mappers(master_server, mapper_process_list, config)

    # print(f"\n[MASTER] All {mapper_count} mapper tasks are complete...\n")
    # logging.info(f"All {mapper_count} mapper tasks are complete...")

    # # Start reducers
    # print(f"[MASTER] Starting {reducer_count} Reducers...")
    # logging.info(f"Starting {reducer_count} Reducers...")
    # reducer_process_list = start_reducers(config, reduce_func)

    # # Wait for all reducers to complete: only applicable if single output file is desired
    # wait_for_reducers(master_server, reducer_process_list, config)

    # print(f"[MASTER] Generating final output file & writing to {config['final_output_path']}...")
    # logging.info(f"Generating final output file & writing to {config['final_output_path']}...")
    # # Combine reducers' output into a single file
    # combine_reducer_output(kv_store_addr)


    # # Terminate all mappers/reducers and the KV store
    # for process in mapper_process_list:
    #     if process.is_alive():
    #         process.terminate()
    # for process in reducer_process_list:
    #     if process.is_alive():
    #         process.terminate()
    # kv_store_process.terminate()


if __name__ == "__main__":
    master_init()
    
    







    

