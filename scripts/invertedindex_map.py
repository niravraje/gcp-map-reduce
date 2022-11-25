from collections import defaultdict
import logging

def invertedindex_map_init(dataset, mapper_id):
    print(f"[MAPPER - {mapper_id}] Inverted index mapping started...")
    logging.info(f"[{mapper_id}] Inverted index reducing started...")

    output = defaultdict(set)
    for doc in dataset:
        for line in dataset[doc]:
            tokens = line.split()
            for token in tokens:
                output["default_mapper_key"].add((token, doc))
    
    for key, val in output.items():
        output[key] = list(val)
    
    return output