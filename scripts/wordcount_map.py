from collections import defaultdict
import logging

def wordcount_map_init(dataset, mapper_id):
    print(f"[MAPPER - {mapper_id}] Word count mapping started...")
    logging.info(f"[{mapper_id}] Word count mapping started...")
    output = defaultdict(list)
    i = 0
    for doc in dataset:
        for line in dataset[doc]:
            tokens = line.split()
            for token in tokens:
                output[token].append(1)
    
    # print("[MAPPER]-[WORD COUNT] Intermediate Output\n", output)
    return output

