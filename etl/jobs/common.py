from functools import partial


def pipe(data, *funcs):
    """Pipe a value through a sequence of functions"""
    for func in funcs:
        data = func(data)
    return data
