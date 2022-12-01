import json

with open("datasetgenerated.json") as fp:
    data = json.load(fp)
    print(data.keys())