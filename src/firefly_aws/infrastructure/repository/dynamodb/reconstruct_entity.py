from __future__ import annotations

from typing import Type

import firefly as ff


class ReconstructEntity(ff.Dependency):
    def __call__(self, type_: Type[ff.Entity], data: list):
        pass
