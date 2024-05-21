import numpy as np
from multiprocessing import Pool
from time import sleep


print('testing')
sleep(1)

def smart_parallelize(func, args2parallelize, *args):
    args2par = list(args[:args2parallelize])
    argsnotpar = [list(args[args2parallelize:])*len(args2par[0])]
    if len(argsnotpar[0]) != 0:
        inputs = zip(*args2par, *argsnotpar)
    else:
        inputs = zip(*args2par)
    with Pool() as p:
        out = p.starmap(func, inputs)
    return out


if __name__ == "__main__":
    import timeit

    def func(x, y):
        # y = 1
        sleep(1)
        x = np.array(x)
        y = np.array(y)

        print(x, y)
        return x+y

    def f3(x, y, z):
        # y = 1
        sleep(1)
        x = np.array(x)
        y = np.array(y)

        print(x, y)
        return x+y+z

    def f2(x):
        y = 1
        sleep(1)
        x = np.array(x)
        y = np.array(y)

        return x+y

    
    x = list(np.ones((20,2)))
    y = list(np.ones((20,2))*3)
    z = 2
    start  = timeit.default_timer()

    a1 = smart_parallelize(f2, 1, x)
    answer = smart_parallelize(func, 2, x, y)
    a3 = smart_parallelize(f3, 2, x, y, z)
    
    stop  = timeit.default_timer()
    print(stop - start)
    print(answer)

    # for i,x_val in enumerate(x):
    #     print(func(x_val))