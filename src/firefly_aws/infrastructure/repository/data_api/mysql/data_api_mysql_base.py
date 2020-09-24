from __future__ import annotations

from abc import ABC
from typing import Type

import firefly.domain as ffd
import firefly.infrastructure as ffi
from firefly.infrastructure.repository.rdb_repository import Index, Column

from ..data_api_storage_interface import DataApiStorageInterface


class DataApiMysqlBase(DataApiStorageInterface, ffi.LegacyStorageInterface, ABC):
    def _get_table_indexes(self, entity: Type[ffd.Entity]):
        result = self._execute(*self._generate_query(entity, 'mysql/get_indexes.sql'))
        indexes = {}
        if result:
            for row in result:
                name = row['INDEX_NAME']
                if name == 'PRIMARY':
                    continue
                column = row['COLUMN_NAME']
                if name not in indexes:
                    indexes[name] = Index(name=name, columns=[column], unique=row['NON_UNIQUE'] == 0)
                else:
                    indexes[name].columns.append(column)

        return indexes.values()

    def _get_table_columns(self, entity: Type[ffd.Entity]):
        result = self._execute(*self._generate_query(entity, f'{self._sql_prefix}/get_columns.sql'))
        ret = []
        if result:
            for row in result:
                ret.append(Column(name=row['COLUMN_NAME'], type=row['COLUMN_TYPE']))
        return ret
