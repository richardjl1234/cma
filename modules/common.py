import logging

# create a decorator timeit to record the time cost of the function
def timeit(func):
    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logging.info(f'{func.__name__} took {end - start} seconds to execute')
        return result
    return wrapper  

