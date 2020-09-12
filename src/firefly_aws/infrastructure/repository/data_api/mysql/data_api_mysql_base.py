from __future__ import annotations

from abc import ABC
from typing import Type

import firefly.domain as ffd
from firefly.infrastructure.repository.rdb_repository import Index

from ..data_api_storage_interface import DataApiStorageInterface


class DataApiMysqlBase(DataApiStorageInterface, ABC):
    def _get_table_indexes(self, entity: Type[ffd.Entity]):
        result = self._execute(*self._generate_query(entity, 'mysql/get_indexes.sql'))
        indexes = {}
        if result:
            for row in result['records']:
                name = list(row[2].values())[0]
                if name == 'PRIMARY':
                    continue
                column = list(row[4].values())[0]
                if name not in indexes:
                    indexes[name] = Index(name=name, columns=[column], unique=list(row[1].values())[0] == 0)
                else:
                    indexes[name].columns.append(column)

        return indexes.values()
