from __future__ import annotations

import inspect
from typing import Union, Type

import firefly as ff


class GeneratePk(ff.Dependency):
    def __call__(self, entity: Union[ff.Entity, Type[ff.Entity]], id_: str = None):
        if inspect.isclass(entity):
            return f"{entity.__name__}#{id_}"
        return f"{entity.__class__.__name__}#{entity.id_value()}"
