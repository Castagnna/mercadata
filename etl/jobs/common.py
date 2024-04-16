from functools import partial


def pipe(data, *functions):
    """Pipe a value through a sequence of functions"""
    for fn in functions:
        data = fn(data)
    return data


def upartial(fn, *args, **kwargs):
    """An upgraded functools partial which accepts positional arguments."""
    params = fn.__code__.co_varnames[1:]
    kwargs = {**{param: arg for param, arg in zip(params, args)}, **kwargs}
    return partial(fn, **kwargs)
