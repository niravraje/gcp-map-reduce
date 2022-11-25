import logging

def wordcount_reduce_init(reducer_input, reducer_id):
    print(f"[REDUCER - {reducer_id}] Word count reducing started...")
    logging.info(f"[{reducer_id}] Word count reducing started...")

    reducer_output = {}
    for key, val in reducer_input.items():
        reducer_output[key] = sum(val)
    
    return reducer_output

