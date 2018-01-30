from typing import Callable

from . import utils


def show_component_version(printer: Callable):
    def decorator(f):
        def wrapped(*args):
            component_version = utils.get_current_component_version()[:7]
            printer(component_version)
            f(*args)
        return wrapped
    return decorator
