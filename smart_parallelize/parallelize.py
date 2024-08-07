from time import sleep

from pathos.multiprocessing import Pool


class _parallelize:
    def __init__(self, func=None, *, args2parallelize=1, DEBUG=False):
        self.func = func
        self.args2parallelize = args2parallelize

        self.DEBUG = DEBUG
        if func:
            self.__call__ = self._decorator(func)

    def _decorator(self, func):
        def wrapper(*args):
            args2parallelize = self.args2parallelize
            func = self.func

            args2par = list(args[:args2parallelize])
            args2par2 = []
            for i, a in enumerate(args2par[0]):
                c = []
                for j in range(args2parallelize):
                    c.append(args2par[j][i])
                args2par2.append(tuple(c))
            args2par = args2par2
            argsnotpar = list(([args[args2parallelize:]])) * len(args2par)

            if len(argsnotpar[0]) != 0:
                inputs = [a + argsnotpar[i] for i, a in enumerate(args2par)]
            else:
                inputs = args2par

            debug = self.DEBUG
            if not debug:
                with Pool() as p:
                    out = p.starmap(func, inputs)
            else:
                out = [func(*a) for a in inputs]
            return out
        return wrapper

    def __call__(self, *args, **kwargs):
        return self._decorator(self.func)(*args, **kwargs)

def parallelize_decorator(func=None, *, args2parallelize=1, DEBUG=False):
    if func:
        return _parallelize(func, args2parallelize=args2parallelize, DEBUG=DEBUG)
    else:
        def wrapper(f):
            return _parallelize(f, args2parallelize=args2parallelize, DEBUG=DEBUG)
        return wrapper

@parallelize_decorator
def func1(x: int):
    sleep(0.5)
    return x

@parallelize_decorator
def func2(x: int, y: int):
    sleep(0.5)
    return x

@parallelize_decorator
def func2p5(x: int, y: int):
    sleep(0.5)
    return x, y

@parallelize_decorator
def func3(x:list):
    sleep(0.5)
    return x

@parallelize_decorator
def func4(x: list, y: int):
    sleep(0.5)
    return x

@parallelize_decorator
def func4p5(x: list, y: int):
    sleep(0.5)
    return x, y

@parallelize_decorator(args2parallelize=2)
def func5(x: list, y: list):
    sleep(0.5)
    return x

@parallelize_decorator(args2parallelize=2)
def func5p5(x: list, y: list):
    sleep(0.5)
    return x, y


if __name__ == "__main__":
    x_int = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    y_int = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    x_list = [x_int, x_int, x_int, x_int, x_int, x_int, x_int, x_int, x_int, x_int]
    y_list = [x_int, x_int, x_int, x_int, x_int, x_int, x_int, x_int, x_int, x_int]

    x_singleint = 1
    y_singleint = 1

    print(func1(x_int))
    print(func2(x_int, y_singleint))
    print(func2p5(x_int, y_singleint))
    print(func3(x_list))
    print(func4(x_list, y_int))
    print(func4p5(x_list, y_int))
    print(func5(x_list, y_list))
    print(func5p5(x_list, y_list))