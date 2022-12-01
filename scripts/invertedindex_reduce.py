import logging

def invertedindex_reduce_init(reducer_input, reducer_id):
    print(f"[REDUCER - {reducer_id}] Inverted index reducing started...")
    logging.info(f"[{reducer_id}] Inverted index reducing started...")

    reducer_output = {}
    for item in reducer_input:
        token, doc_name = item
        if token not in reducer_output:
            reducer_output[token] = set()
        reducer_output[token].add(doc_name)
    
    for key, val in reducer_output.items():
        reducer_output[key] = list(val)
    
    return reducer_output

