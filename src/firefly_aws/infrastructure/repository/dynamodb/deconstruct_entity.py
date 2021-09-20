from __future__ import annotations

import inspect
from dataclasses import fields
from typing import get_type_hints, List, Dict, Union

import firefly as ff


class DeconstructEntity(ff.Dependency):
    # noinspection PyDataclass
    def __call__(self, entity: ff.Entity, path: list = None, pk: str = None):
        path = path or []
        pk = pk or f"{entity.__class__.__name__}#{entity.id_value()}"
        sk = '.'.join(path)
        ret = []
        data = {}

        types = get_type_hints(entity.__class__)
        for field_ in fields(entity):
            if field_.name.startswith('_'):
                continue

            val = getattr(entity, field_.name)
            t = types[field_.name]
            if ff.is_type_hint(t) and ff.get_origin(t) in (List, Dict) and self._hint_contains_entity(t) and \
                    val is not None:
                self._process_collection(t, field_, entity, path, pk, ret)

            elif ff.is_type_hint(t) and ff.get_origin(t) is Union:
                val = getattr(entity, field_.name)
                for subtype in ff.get_args(ff.get_origin(t)):
                    if subtype in (List, Dict) and self._hint_contains_entity(subtype) and val is not None:
                        self._process_collection(subtype, field_, entity, path, pk, ret)
                    elif inspect.isclass(subtype) and val is not None and isinstance(val, subtype):
                        self._process_entity(field_, entity, path, pk, ret)

            else:
                if inspect.isclass(t) and issubclass(t, ff.Entity):
                    self._process_entity(field_, entity, path, pk, ret)
                else:
                    self._process_primitive_type(t, field_, entity, data)

        data['pk'] = pk
        data['sk'] = sk
        if sk == '':
            data['sk'] = 'root'
        ret.append(data)

        return ret

    def _process_entity(self, field_, entity, path, pk, ret):
        val = getattr(entity, field_.name)
        if val is not None:
            p = path.copy()
            p.append(field_.name)
            ret.extend(self(val, p, pk=pk))

    def _process_primitive_type(self, type_, field_, entity, data):
        if inspect.isclass(type_) and issubclass(type_, ff.ValueObject):
            val = getattr(entity, field_.name)
            if val is not None:
                data[field_.name] = val.to_dict()
        else:
            data[field_.name] = getattr(entity, field_.name)
        idx = field_.metadata.get('index')
        if idx is not None:
            if not isinstance(idx, int):
                raise ff.ConfigurationError('Dynamodb indexes must be integers')
            data[f'gsi_{idx}'] = str(data[field_.name])

    def _process_collection(self, type_, field_, entity, path, pk, ret):
        o = ff.get_origin(type_)
        if o is List:
            p = f"{field_.name}[{{}}]"
            for i, e in enumerate(getattr(entity, field_.name)):
                ret.extend(self(e, path + [p.format(i)], pk=pk))
        elif o is Dict:
            for k, e in getattr(entity, field_.name).items():
                ret.extend(self(e, path + [f"{field_.name}{{{k}}}"], pk=pk))

    def _hint_contains_entity(self, t):
        return len(list(filter(lambda x: (inspect.isclass(x) and issubclass(x, ff.Entity)), ff.get_args(t)))) > 0
