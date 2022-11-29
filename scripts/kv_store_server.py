import socket
import threading
import json
import pickle
import os
import glob
import logging

SIZE = 4096

def count_lines_in_dataset(dataset):
    count = 0
    for doc in dataset:
        count += len(dataset[doc])
    return count

def create_partitioned_dataset(dataset, mapper_count):
    result_dataset = {}
    for mapper_num in range(mapper_count):
        mapper_id = "mapper" + str(mapper_num+1)
        result_dataset[mapper_id] = {}

    total_lines = count_lines_in_dataset(dataset)
    mapper_chunk_size = total_lines // mapper_count

    mapper_num = 1
    mapper_id = "mapper" + str(mapper_num)
    curr_chunk_size = mapper_chunk_size
    start = 0
    mapper_lines_count = 0
    doc_names = list(dataset.keys())

    i = 0
    while i < len(doc_names):

        doc = doc_names[i]
        end = start + curr_chunk_size

        # if end is within doc size limits and this is not the last mapper
        if end < len(dataset[doc]) and mapper_num != mapper_count:
            result_dataset[mapper_id][doc] = dataset[doc][start:end]
            mapper_lines_count += len(dataset[doc][start:end])
            curr_chunk_size = mapper_chunk_size
            start = end
        else:
            result_dataset[mapper_id][doc] = dataset[doc][start:]
            curr_chunk_size = mapper_chunk_size - len(dataset[doc][start:])
            mapper_lines_count += len(dataset[doc][start:])
            start = 0
            i += 1

        if mapper_lines_count == mapper_chunk_size:
            mapper_num += 1
            mapper_id = "mapper" + str(mapper_num)
            mapper_lines_count = 0
    
    print("\n\n\n-- [INFO] Partitioned Dataset Summary --")
    for mapper_id in result_dataset:
        print(f"\n[{mapper_id}]")
        print("Total lines in mapper: ", count_lines_in_dataset(result_dataset[mapper_id]))
        print("Docs in mapper: ", result_dataset[mapper_id].keys())
    print("\n-- End of Partitioned Dataset Summary --\n\n\n")
        
    return result_dataset


def generate_reducer_input_wordcount(group, config, reducer_id):
    category_path = config["mapper_output_path"]
    response = {}
    for i in range(config["mapper_count"]):
        filename = "mapper" + str(i+1) + ".json"
        file_path = os.path.join(category_path, filename)
        print(f"[KV] Generating {reducer_id} input. Reading mapper input from {file_path}")
        logging.info(f"Generating {reducer_id} input. Reading mapper input from {file_path}")
        try:
            with open(file_path, "r") as fp:
                curr_mapper_dict = json.load(fp)
                for key, val in curr_mapper_dict.items():
                    if key[0].lower() in group:
                        if key not in response:
                            response[key] = val
                        else:
                            response[key].extend(val)
        except:
            response = "INVALID_MAPPER_FILE_PARAMETERS"
            logging.error(f"[{reducer_id}] INVALID_MAPPER_FILE_PARAMETERS")
    return response

def generate_reducer_input_invertedindex(group, config, reducer_id):
    category_path = config["mapper_output_path"]
    response = []
    for i in range(config["mapper_count"]):
        filename = "mapper" + str(i+1) + ".json"
        file_path = os.path.join(category_path, filename)
        print(f"[KV] Generating {reducer_id} input. Reading mapper input from {file_path}")
        logging.info(f"Generating {reducer_id} input. Reading mapper input from {file_path}")
        try:
            with open(file_path, "r") as fp:
                token_to_doc_pairs = json.load(fp)["default_mapper_key"]
                
                for item in token_to_doc_pairs:
                    token, doc_name = item
                    if token[0].lower() in group:
                        response.append(item)
        except:
            response = "INVALID_MAPPER_FILE_PARAMETERS"
            logging.error(f"[{reducer_id}] INVALID_MAPPER_FILE_PARAMETERS")
    return response

def client_handler(conn, client_addr, config):
    print(f"[KV] Client {client_addr} has connected.")
    logging.info(f"Client {client_addr} has connected.")
    response = "DONE"

    serialized_msg = b""
    while True:
        packet = conn.recv(SIZE)
        logging.info(f"[KV] Packet received of size {len(packet)}: {packet}")
        serialized_msg += packet

        if b"ENDOFDATA" in packet:
            logging.info(f"ENDOFDATA received. Breaking...")
            break

    serialized_msg = serialized_msg[:-9] # exclude ENDOFDATA
    payload = pickle.loads(serialized_msg)

    print(f"[KV] Command & category received from client {client_addr}:\n", payload[:2])
    logging.info(f"Command & category received from client {client_addr}: {payload[:2]}")

    if payload[0] == "set":
        category = payload[1] # input/mapper/reducer
        
        if category == "input":
            dataset = payload[2]
            mapper_count = payload[3]
            
            partitioned_dataset = create_partitioned_dataset(dataset, mapper_count)

            category_path = config["input_data_path"]

            # open file for writing, create file if doesn't exist
            for mapper_id in partitioned_dataset:
            
                filename = "input-" + str(mapper_id) + ".json"
                file_path = os.path.join(category_path, filename)
                print(f"[KV] Generating {mapper_id} input & writing to {file_path}")
                logging.info(f"Generating {mapper_id} input & writing to {file_path}")
                with open(file_path, 'w') as fp:
                    try:
                        json.dump(partitioned_dataset[mapper_id], fp, indent=4)
                        response = "STORED\r\n"
                    except:
                        response = "NOT_STORED\r\n"
                        print(f"[KV] Error in writing mapper input to {file_path}. Response: {response}")
                        logging.error(f"Error in writing mapper input to {file_path}. Response: {response}")

        elif category == "mapper-output":
            mapper_id = payload[2]
            mapper_output = payload[3]

            category_path = config["mapper_output_path"]

            filename = str(mapper_id) + ".json"
            file_path = os.path.join(category_path, filename)
            print(f"[KV] Generating {mapper_id} output & writing to {file_path}")
            logging.info(f"Generating {mapper_id} output & writing to {file_path}")

            try:
                with open(file_path, 'w') as fp:
                    json.dump(mapper_output, fp)
                    response = "STORED\r\n"
            except:
                response = "NOT_STORED\r\n"
                print(f"[KV] Error in writing mapper_output to {file_path}. Response: {response}")
                logging.info(f"Error in writing mapper_output to {file_path}. Response: {response}")

        elif category == "reducer-output":
            reducer_id = payload[2]
            reducer_output = payload[3]

            category_path = config["reducer_output_path"]
            filename = str(reducer_id) + ".json"
            file_path = os.path.join(category_path, filename)
            print(f"[KV] Generating {reducer_id} output & writing to {file_path}")
            logging.info(f"Generating {reducer_id} output & writing to {file_path}")

            try:
                with open(file_path, 'w') as fp:
                    json.dump(reducer_output, fp)
                    response = "STORED\r\n"
            except:
                response = "NOT_STORED\r\n"
                print(f"[KV] Error in writing reducer output to {file_path}. Response: {response}")
                logging.error(f"Error in writing reducer output to {file_path}. Response: {response}")

        
    elif payload[0] == "get":
        category = payload[1] # input/mapper/reducer

        if category == "input":
            mapper_id = payload[2]
            print("[KV] Mapper ID received: ", mapper_id)
            logging.info(f"Mapper ID received: {mapper_id}")

            category_path = config["input_data_path"]
            filename = "input-" + str(mapper_id) + ".json"
            file_path = os.path.join(category_path, filename)
            print(f"[KV] Retrieving {mapper_id} input from {file_path}")
            logging.info(f"Retrieving {mapper_id} input from {file_path}")
            try:
                with open(file_path, "r") as fp:
                    response = json.load(fp)
            except:
                response = "INVALID_MAPPER_ID"
                print(f"[KV] Error in retrieving mapper input from {file_path}. Response: {response}")
                logging.error(f"Error in retrieving mapper input from {file_path}. Response: {response}")
            
        elif category == "mapper-output":
            group = set(payload[2])
            reducer_id = payload[3]
            print(f"\n[KV] Alphabet group for {reducer_id}: {group}\n")
            logging.info(f"Alphabet group for {reducer_id}: {group}\n")

            if config["operation_name"] == "wordcount":
                response = generate_reducer_input_wordcount(group, config, reducer_id)
            elif config["operation_name"] == "invertedindex":
                response = generate_reducer_input_invertedindex(group, config, reducer_id)
            else:
                response = "INVALID_OPERATION_NAME"
                print(f"[KV] Error in retrieving mapper output from {file_path}. Response: {response}")
                logging.error(f"Error in retrieving mapper output from {file_path}. Response: {response}")
            
    elif payload[0] == "combine":
        category = payload[1]
        if category == "final-output":
            category_path = config["final_output_path"]
            
            final_output = {}
            reducer_file_paths = glob.glob(os.path.join(config["reducer_output_path"], "reducer*.json"))
            print(f"\n[KV] Combining reducer output files into a single file. Reading below reducer files:\n{reducer_file_paths}\n")
            logging.info(f"Combining reducer output files into a single file. Reading below reducer files:\n{reducer_file_paths}\n")
            for reducer_file in reducer_file_paths:
                try:
                    with open(reducer_file, "r") as fp:
                        curr_reducer_output = json.load(fp)
                        for key, val in curr_reducer_output.items():
                            final_output[key] = val
                except:
                    response = "REDUCER_OUTPUT_READ_ERROR"
                    print(f"[KV] Error in combining reducer output files. Read error for {file_path}. Response: {response}")
                    logging.error(f"Error in combining reducer output files. Read error for {file_path}. Response: {response}")


            final_output = dict(sorted(final_output.items()))
            final_output_filename = "final-output-" + config["operation_name"] + ".json"
            final_output_file_path = os.path.join(category_path, final_output_filename)
            try:
                with open(final_output_file_path, "w") as fp:
                    json.dump(final_output, fp, indent=4)
                    response = "STORED\r\n"
            except:
                response = "NOT_STORED\r\n"
                print(f"[KV] Error in combining reducer output files. Error while writing to file {file_path}. Response: {response}")
                logging.info(f"Error in combining reducer output files. Error while writing to file {file_path}. Response: {response}")

    elif payload[0] == "cleanup":
        print("\n[KV] Cleaning up KV Store's data from previous runs\n")
        logging.info("Cleaning up KV Store's data from previous runs\n")
        # delete any previous mapper input files
        folder_files = glob.glob(os.path.join(config["input_data_path"], "*"))
        for f in folder_files:
            os.remove(f)

        # delete any previous mapper output files if present
        folder_files = glob.glob(os.path.join(config["mapper_output_path"], "*"))
        for f in folder_files:
            os.remove(f)

        # delete any previous reducer output files if present
        folder_files = glob.glob(os.path.join(config["reducer_output_path"], "*"))
        for f in folder_files:
            os.remove(f)

        # delete any previous final output files if present
        curr_operation_pattern = "final-output-" + config["operation_name"] + "*.json"
        folder_files = glob.glob(os.path.join(config["final_output_path"], curr_operation_pattern))
        for f in folder_files:
            os.remove(f)

        print("\n[KV] All intermediate files from previous sessions cleaned\n")
        logging.info("All intermediate files from previous sessions cleaned\n")
    else:
        response = "[KV] CLIENT_ERROR Invalid Command Received\r\n"

    conn.send(pickle.dumps(response) + b"ENDOFDATA")
    
    print(f"[KV] Connection to {client_addr} closed.")
    conn.close()

def start_kv_server(config):

    # initialize logging configurations
    logging.basicConfig(
        filename=config["kvstore_log_path"], 
        filemode='w', 
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.DEBUG
        )

    kv_store_ip = socket.gethostbyname(socket.gethostname())
    kv_store_port = config["kv_store_port"]
    kv_store_addr = (kv_store_ip, kv_store_port)
    
    print("[KV] KV Store Server started...")
    logging.info("[KV] KV Store Server started...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(kv_store_addr)
    server.listen()
    print(f"[KV] Server listening for connections at address {kv_store_ip}:{kv_store_port}")
    logging.info(f"Server listening for connections at address {kv_store_ip}:{kv_store_port}")

    while True:
        conn, client_addr = server.accept()
        print(f"[KV] Connected to client {client_addr}")
        logging.info(f"Connected to client {client_addr}")
        thread = threading.Thread(target=client_handler, args=(conn, client_addr, config,))
        thread.start()
        print(f"[KV] KV Number of clients connected: {threading.active_count() - 1}")
        logging.info(f"KV Number of clients connected: {threading.active_count() - 1}")

if __name__ == "__main__":
    print("curr dir", os.getcwd())
    with open("./gcp-map-reduce/config.json", "r") as fp:
        config = json.load(fp)

    start_kv_server(config)