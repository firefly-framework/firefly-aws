from __future__ import annotations

from typing import Optional, Union, Callable, Tuple, List

import firefly as ff
from firefly import infrastructure
from firefly import domain as ffd, Repository
from firefly.domain.repository.repository import T

from .dynamodb_storage_interface import DynamodbStorageInterface

DEFAULT_LIMIT = 999999999999999999


class DynamodbRepository(ff.Repository[T]):
    _interface: DynamodbStorageInterface = None
    _ddb_client = None
    _cache: dict = {
        'entity_composition': {},
    }

    def __init__(self, interface: DynamodbStorageInterface):
        super().__init__()
        self._entity_type = self._type()
        self._query_details = {}
        self._interface = interface

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
        ret = self._interface.find(x, self._type())
        if isinstance(ret, ff.AggregateRoot):
            self.register_entity(ret)

        return ret

    def filter(self, x: Union[Callable, ffd.BinaryOp], **kwargs) -> Repository:
        criteria = x
        if not isinstance(criteria, ff.BinaryOp):
            criteria = x(ff.EntityAttributeSpy())
        ret = self._interface.all(self._type(), criteria)
        list(map(lambda e: self.register_entity(e), ret))

        return ret

    def commit(self, force_delete: bool = False):
        self.debug('commit() called in %s', str(self))

        if len(self._deletions) > 0:
            self.debug('Deleting %s', self._deletions)
            self._interface.remove(self._deletions, force=force_delete)

        new_entities = self._new_entities()
        if len(new_entities) > 0:
            self.debug('Adding %s', new_entities)
            if self._interface.add(new_entities) != len(new_entities):
                raise ffd.ConcurrentUpdateDetected()

        for entity in self._changed_entities():
            self.debug('Updating %s', entity)
            if self._interface.update(entity) == 0:
                raise ffd.ConcurrentUpdateDetected()
        self.debug('Done in commit()')

    def sort(self, cb: Optional[Union[Callable, Tuple[Union[str, Tuple[str, bool]]]]], **kwargs):
        pass

    def clear(self):
        self._interface.clear(self._entity_type)

    def destroy(self):
        self._interface.destroy(self._entity_type)

    def copy(self):
        ret = self.__class__()
        ret._query_details = self._query_details.copy()
        ret._entities = []
        ret._entity_hashes = {}
        ret._deletions = []
        ret._parent = self

        deletions = self._deletions
        entities = self._new_entities()
        self.reset()
        self._deletions = deletions
        self._entities = entities

        return ret

    def __iter__(self):
        if 'raw' in self._query_details and self._query_details['raw'] is True:
            return iter(self._load_data())
        self._load_data()
        return iter(list(self._entities))

    def __len__(self):
        params = self._query_details.copy()
        if 'criteria' in params and not isinstance(params['criteria'], ffd.BinaryOp):
            params['criteria'] = self._get_search_criteria(params['criteria'])
        return self._interface.all(self._entity_type, count=True, **params)

    def __getitem__(self, item):
        if isinstance(item, slice):
            if item.start is not None:
                self._query_details['offset'] = item.start
            if item.stop is not None:
                self._query_details['limit'] = (item.stop - item.start) + 1
            else:
                self._query_details['limit'] = DEFAULT_LIMIT
        else:
            if len(self._entities) > item:
                return self._entities[item]
            self._query_details['offset'] = item
            self._query_details['limit'] = 1

        if 'raw' in self._query_details and self._query_details['raw'] is True:
            return self._load_data()

        self._load_data()

        if isinstance(item, slice):
            return self._entities
        elif len(self._entities) > 0:
            return self._entities[-1]

    def _load_data(self):
        query_details = self._query_details

        if 'criteria' not in query_details:
            query_details['criteria'] = None

        results = self._do_filter(**query_details)
        if 'raw' in query_details and query_details['raw'] is True:
            return results

        if isinstance(results, list):
            for entity in results:
                if entity not in self._entities:
                    self.register_entity(entity)

    def _do_delete(self, entity: ff.Entity):
        pass
