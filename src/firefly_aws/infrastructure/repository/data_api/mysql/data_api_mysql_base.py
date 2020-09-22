from __future__ import annotations

from abc import ABC
from typing import Type, Union, Tuple, Callable

import firefly.domain as ffd
import firefly.infrastructure as ffi
from botocore.exceptions import ClientError
from firefly.infrastructure.repository.rdb_repository import Index

import firefly_aws.domain as domain
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

    def _add(self, entity: ffd.Entity):
        try:
            return super()._add(entity)
        except domain.DocumentTooLarge:
            self._insert_large_document(entity)

    def _all(self, entity_type: Type[ffd.Entity], criteria: ffd.BinaryOp = None, limit: int = None, offset: int = None,
             sort: Tuple[Union[str, Tuple[str, bool]]] = None, raw: bool = False, count: bool = False):
        try:
            return super()._all(
                entity_type, criteria, limit=limit, offset=offset, sort=sort, raw=raw, count=count
            )
        except ClientError as e:
            if 'Database returned more than the allowed response size limit' in str(e):
                sql, params, pruned_criteria = self._generate_select(
                    entity_type, criteria, limit=limit, offset=offset, sort=sort, count=count
                )
                return self._fetch_multiple_large_documents(sql, params, entity_type)
            raise e

    def _find(self, uuid: Union[str, Callable], entity_type: Type[ffd.Entity]):
        try:
            return super()._find(uuid, entity_type)
        except ClientError as e:
            if 'Database returned more than the allowed response size limit' in str(e):
                return self._fetch_large_document(uuid, entity_type)
            raise e

    def _remove(self, entity: ffd.Entity):
        return super()._remove(entity)

    def _update(self, entity: ffd.Entity):
        try:
            return super()._update(entity)
        except domain.DocumentTooLarge:
            self._insert_large_document(entity, update=True)
