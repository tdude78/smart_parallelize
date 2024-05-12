from functools import wraps
from time import sleep

import numpy as np
import ray
from ray.exceptions import RaySystemError


def get_mem_func():
    try:
        MEMORY = ray.available_resources()['memory']
    except ray.exceptions.RaySystemError:
        ray.init()
        MEMORY = ray.available_resources()['memory']
    try:
        CPUS = ray.available_resources()['CPU']
        MEM_PER_WORKER = (MEMORY / CPUS) * 0.8
    except KeyError:
        # get number of cores
        import multiprocessing
        CPUS = multiprocessing.cpu_count()
        MEM_PER_WORKER = (MEMORY / CPUS) * 0.8
    MEMORY = int(MEMORY)
    CPUS   = int(CPUS)
    MEM_PER_WORKER = int(MEM_PER_WORKER)
    return MEMORY, CPUS, MEM_PER_WORKER

try:
	MEMORY, CPUS, MEM_PER_WORKER = get_mem_func()
except RaySystemError:
	MEMORY, CPUS, MEM_PER_WORKER = get_mem_func()


def smart_parallelize(args2parallelize):
    def decorator(function):
        @wraps(function)
        def wrapper(**kwargs):
            @ray.remote(memory=MEM_PER_WORKER)
            def get_results(**kwargs):
                par_args = list(kwargs.keys())[:args2parallelize]
                other_args = list(kwargs.keys())[args2parallelize:]
                data_par_args = [kwargs[i] for i in par_args]
                print(par_args, data_par_args)

                results = []
                for i, _ in enumerate(data_par_args[0]):
                    kwargs_0 = kwargs.copy()
                    for j, arg in enumerate(par_args):
                        kwargs_0[arg] = data_par_args[j][i]
                    for k, arg in enumerate(other_args):
                        kwargs_0[arg] = kwargs[arg]
                    r = function(**kwargs_0)
                    results.append(r)
                return results
            
            par_args = list(kwargs.keys())[:args2parallelize]
            other_args = list(kwargs.keys())[args2parallelize:]
            arg2parallelize_unsplit = [kwargs[i] for i in par_args]

            # split into CPUS chunks
            split_args = [np.array_split(i, CPUS) for i in arg2parallelize_unsplit]

            workers = []
            for i in range(CPUS):
                inp_args = {}
                for j, _ in enumerate(par_args):
                    inp_args[par_args[j]] = split_args[j][i]
                
                for k, _ in enumerate(other_args):
                    inp_args[other_args[k]] = kwargs[other_args[k]]
                workers.append(get_results.remote(**inp_args))
            
            test_inp = {}
            for j, _ in enumerate(par_args):
                test_inp[par_args[j]] = arg2parallelize_unsplit[j][0]
            for k, _ in enumerate(other_args):
                test_inp[other_args[k]] = kwargs[other_args[k]]
            try:
                num_outputs = len(function(**test_inp))
            except TypeError:
                num_outputs = 1

            retval = ray.get(workers)

            # retval = retval.reshape(len(arg2parallelize_unsplit[0]), num_outputs)

            retval = np.array([j for i in retval for j in i])

            return retval
        return wrapper
    return decorator


if __name__ == "__main__":
    import timeit

    @smart_parallelize(args2parallelize=1)
    def func(x, y):
        return x + y
    
    x = np.ones((31,2))
    # y = np.ones((30,))*2
    y = 2
    start  = timeit.default_timer()
    answer = func(x=x, y=y)
    stop  = timeit.default_timer()
    print(stop - start)
    print(answer)
