import logging

FUNC_TIME_DICT = {}
# create a decorator timeit to record the time cost of the function
def timeit(func):
    func_name = func.__name__
    if func_name not in FUNC_TIME_DICT:
        FUNC_TIME_DICT[func_name] = []

    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        time_spent = end - start
        logging.info(f'{func_name} took {time_spent} seconds to execute')

        FUNC_TIME_DICT[func_name].append(time_spent)
        return result
    return wrapper  

