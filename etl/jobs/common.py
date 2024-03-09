from functools import partial


def pipe(data, *funcs):
    """Pipe a value through a sequence of functions"""
    for func in funcs:
        data = func(data)
    return data

def upartial(f, *args, **kwargs):
    """An upgraded functools partial which accepts positional arguments."""
    params = f.__code__.co_varnames[1:]
    kwargs = {**{param: arg for param, arg in zip(params, args)}, **kwargs}
    return partial(f, **kwargs)
