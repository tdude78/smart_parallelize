from .parallelize import parallelize


@parallelize
def func1(x: int):
    return x

@parallelize
def func2(x: int, y: int):
    return x

@parallelize
def func2p5(x: int, y: int):
    return x, y

@parallelize
def func3(x:list):
    return x

@parallelize(args2parallelize=1)
def func4(x: list, y: int):
    return x

@parallelize(args2parallelize=1)
def func4p5(x: list, y: int):
    return x, y

@parallelize(args2parallelize=2)
def func5(x: list, y: list):
    return x

@parallelize(args2parallelize=2)
def func5p5(x: list, y: list):
    return x, y


if __name__ == "__main__":
    x_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    y_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    x_int = 1
    y_int = 1

    print(func1(x_int))
    print(func2(x_int, y_int))
    print(func2p5(x_int, y_int))
    print(func3(x_list))
    print(func4(x_list, y_int))
    print(func4p5(x_list, y_int))
    print(func5(x_list, y_list))
    print(func5p5(x_list, y_list))