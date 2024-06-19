from smart_parallelize.testing import func1, func2, func2p5, func3, func4, func4p5, func5, func5p5

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