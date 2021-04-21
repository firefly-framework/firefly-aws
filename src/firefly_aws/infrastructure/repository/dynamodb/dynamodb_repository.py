from __future__ import annotations

import inspect
import typing
from dataclasses import fields
from datetime import datetime, date
from pprint import pprint
from typing import Optional, Union, Callable, Tuple, List

import firefly as ff
from firefly import domain as ffd, Repository
from firefly.domain.repository.repository import T


class DynamodbRepository(ff.Repository[T]):
    _ddb_client = None
    _cache: dict = {
        'entity_composition': {},
    }

    def append(self, entity: Union[T, List[T], Tuple[T]], **kwargs) -> DynamodbRepository:
        if isinstance(entity, (list, tuple)):
            list(map(lambda e: self._entities.append(e), entity))
        else:
            self._entities.append(entity)

        return self

    def remove(self, x: Union[T, List[T], Tuple[T], Callable, ffd.BinaryOp], **kwargs):
        if isinstance(x, ff.Entity):
            self._deletions.append(x)
        elif isinstance(x, (list, tuple)):
            list(map(lambda e: self._deletions.append(e), x))
        else:
            list(map(lambda e: self._deletions.append(e), self.filter(x)))

        return self

    def find(self, x: Union[str, Callable, ffd.BinaryOp], **kwargs) -> Optional[T]:
        pass

    def filter(self, x: Union[Callable, ffd.BinaryOp], **kwargs) -> Repository:
        pass

    def commit(self, force_delete: bool = False):
        self.debug('commit() called in %s', str(self))

        if len(self._deletions) > 0:
            self.debug('Deleting %s', self._deletions)
            self._interface.remove(self._deletions, force=force_delete)

        new_entities = self._new_entities()
        if len(new_entities) > 0:
            self.debug('Adding %s', new_entities)
            if self._do_add(new_entities) != len(new_entities):
                raise ffd.ConcurrentUpdateDetected()

        for entity in self._changed_entities():
            self.debug('Updating %s', entity)
            if self._interface.update(entity) == 0:
                raise ffd.ConcurrentUpdateDetected()
        self.debug('Done in commit()')

    def sort(self, cb: Optional[Union[Callable, Tuple[Union[str, Tuple[str, bool]]]]], **kwargs):
        pass

    def clear(self):
        pass

    def destroy(self):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        pass

    def __getitem__(self, item):
        pass

    def _do_delete(self, entity: ff.Entity):
        pass

    def _do_add(self, entities: List[ff.Entity]) -> int:
        for entity in entities:
            self._decompose_entity(entity)

    def _to_item(self, raw):
        if isinstance(raw, ff.ValueObject):
            raw = raw.to_dict()

        if isinstance(raw, dict):
            return {
                'M': {
                    k: self._to_item(v)
                    for k, v in raw.items()
                }
            }
        elif isinstance(raw, list):
            return {
                'L': [self._to_item(v) for v in raw]
            }
        elif isinstance(raw, (date, datetime)):
            return {'S': raw.isoformat()}
        elif raw is None:
            return {'NULL': True}
        elif isinstance(raw, bool):
            return {'BOOL': raw}
        elif isinstance(raw, str):
            return {'S': raw}
        elif isinstance(raw, int):
            return {'N': str(raw)}

    def _decompose_entity(self, entity: ff.Entity, root: ff.Entity = None, attr_chain: List[str] = None) -> List[dict]:
        attr_chain = attr_chain or []
        ret = []
        data = entity.to_dict()
        types = typing.get_type_hints(entity.__class__)

        if entity.__class__.__name__ not in self._cache['entity_composition']:
            comp = {
                'entity_fields': {
                    'single': [],
                    'list': [],
                    'dict': [],
                },
                'normal_fields': [],
            }
            # noinspection PyDataclass
            for field_ in fields(entity.__class__):
                try:
                    t = types[field_.name]
                except KeyError:
                    t = None

                if t is not None:
                    if ff.is_type_hint(t):
                        # Handle Union, List, Dict
                        origin = ff.get_origin(t)
                        args = ff.get_args(t)
                        if origin is List:
                            if issubclass(args[0], ff.Entity):
                                comp['entity_fields']['list'].append(field_.name)
                        elif origin is typing.Dict:
                            if issubclass(args[1], ff.Entity):
                                comp['entity_fields']['dict'].append(field_.name)
                        pass
                    elif issubclass(types[field_.name], ff.Entity):
                        comp['entity_fields']['single'].append(field_.name)
                    else:
                        comp['normal_fields'].append(field_.name)
                else:
                    comp['normal_fields'].append(field_.name)

            self._cache['entity_composition'][entity.__class__.__name__] = comp

        comp = self._cache['entity_composition'][entity.__class__.__name__]
        for name in comp['entity_fields']['single']:
            ret.extend(self._decompose_entity(getattr(entity, name), root or entity, attr_chain + [name]))
            del data[name]

        for name in comp['entity_fields']['list']:
            list(map(
                lambda f: ret.extend(self._decompose_entity(f, root or entity, attr_chain + [name])),
                getattr(entity, name)
            ))

        item = {
            'pk': self._entity_key(entity) if root is None else self._entity_key(root),
            'sk': self._entity_key(entity),
        }
        if len(attr_chain) > 0:
            item['__location'] = self._to_item(':'.join(attr_chain))

        for name in comp['normal_fields']:
            item[name] = self._to_item(getattr(entity, name))
        ret.append(item)

        pprint(ret)
        return ret

    def _entity_key(self, entity: ff.Entity) -> str:
        return f'{entity.__class__.__name__}#{entity.id_value()}'
