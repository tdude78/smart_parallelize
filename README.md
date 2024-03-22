<!-- write a heading for describing this -->
## Description

This is a simply python package for easily parallelizing functions using.


Example code:

    from smart_parallel.parallelize import smart_parallelize
    layout = [ [sg.Text('Hello, world!')] ]
    window = sg.Window('Hello Example', layout)
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED:
            break
    window.close()