from flask import Flask, request, jsonify
from flask_cors import CORS
from master import master_init
import json
import socket
import pickle
import subprocess

app = Flask(__name__)
CORS(app)

SIZE = 4096

@app.route('/', methods=["GET"])
def home():
    return "<h1>GCP Map Reduce Master VM is running!</h1>"

@app.route('/launch_map_reduce', methods=["POST"])
def launch_map_reduce():
    master_init()
    return jsonify({"status": "complete"})

@app.route('/final_output', methods=["GET"])
def fetch_final_output_from_kvstore():
    with open("config.json", "r") as fp:
        config = json.load(fp)
    
    kv_store_addr = (config["kv_store_host"], config["kv_store_port"])
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(kv_store_addr)

    if config["operation_name"] == "invertedindex":
        payload = ("get", "final-output", "invertedindex")
    else:
        payload = ("get", "final-output", "wordcount")

    client.sendall(pickle.dumps(payload) + b"ENDOFDATA")
    serialized_msg = b""
    while True:
        packet = client.recv(SIZE)
        serialized_msg += packet
        if b"ENDOFDATA" in packet:
            break
    serialized_msg = serialized_msg[:-9] # exclude ENDOFDATA
    final_output = pickle.loads(serialized_msg)
    return jsonify(final_output)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="8081", debug=True)
