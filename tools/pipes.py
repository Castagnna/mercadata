from functools import partial
from typing import Any, Callable


def pipe(data: Any, *functions: Callable) -> Any:
    """Pipe a value through a sequence of functions."""
    for fn in functions:
        data = fn(data)
    return data


def upartial(fn: Callable, *args: Any, **kwargs: Any) -> partial:
    """An upgraded functools partial which accepts positional arguments."""
    params = fn.__code__.co_varnames[1:]
    kwargs = {**{param: arg for param, arg in zip(params, args)}, **kwargs}
    return partial(fn, **kwargs)
