#  Copyright (c) 2020 JD Williams
#
#  This file is part of Firefly, a Python SOA framework built by JD Williams. Firefly is free software; you can
#  redistribute it and/or modify it under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 3 of the License, or (at your option) any later version.
#
#  Firefly is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
#  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
#  Public License for more details. You should have received a copy of the GNU Lesser General Public
#  License along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  You should have received a copy of the GNU General Public License along with Firefly. If not, see
#  <http://www.gnu.org/licenses/>.

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import fields
from datetime import datetime
from math import floor
from typing import Type

import firefly as ff
import firefly.infrastructure as ffi
from botocore.exceptions import ClientError
from firefly import domain as ffd

from ..service.data_api import DataApi


class DataApiStorageInterface(ffi.RdbStorageInterface, ABC):
    _cache: dict = None
    _rds_data_client = None
    _serializer: ffi.JsonSerializer = None
    _data_api: DataApi = None
    _size_limit: int = 1000  # In KB

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._select_limits = {}

    def _disconnect(self):
        pass

    def _add(self, entity: ff.Entity):
        sql, params = self._generate_insert(entity)
        ff.retry(lambda: self._data_api.execute(sql, params))

    def _all(self, entity_type: Type[ff.Entity], criteria: ff.BinaryOp = None, limit: int = None):
        sql = f"select {self._generate_select_list(entity_type)} from {self._fqtn(entity_type)}"
        params = []
        if criteria is not None:
            clause, params = self._generate_where_clause(criteria)
            sql = f'{sql} {clause}'

        if limit is not None:
            sql += f" limit {limit}"

        return self._paginate(sql, params, entity_type)

    def _find(self, uuid: str, entity_type: Type[ff.Entity]):
        sql = f"select {self._generate_select_list(entity_type)} from {self._fqtn(entity_type)} where id = :id"
        params = [{'name': 'id', 'value': {'stringValue': uuid}}]
        result = ff.retry(
            lambda: self._data_api.execute(sql, params),
            should_retry=lambda err: 'Database returned more than the allowed response size limit' not in str(err)
        )
        if len(result['records']) == 0:
            return None

        return self._build_entity(entity_type, result['records'][0])

    def _remove(self, entity: ff.Entity):
        sql = f"delete from {self._fqtn(entity.__class__)} where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
        ]
        ff.retry(lambda: self._data_api.execute(sql, params))

    def _update(self, entity: ff.Entity):
        sql, params = self._generate_update(entity)
        ff.retry(lambda: self._data_api.execute(sql, params))

    def _generate_insert(self, entity: ff.Entity, part: str = None):
        t = entity.__class__
        sql = f"insert into {self._fqtn(t)} ({self._generate_column_list(t)}) values ({self._generate_value_list(t)})"
        return sql, self._generate_parameters(entity, part=part)

    def _generate_update(self, entity: ff.Entity, part: str = None):
        t = entity.__class__
        sql = f"update {self._fqtn(t)} set {self._generate_update_list(t)} where id = :id"
        return sql, self._generate_parameters(entity, part=part)

    def _get_indexes(self, entity: Type[ff.Entity]):
        if entity not in self._cache['indexes']:
            self._cache['indexes'][entity] = []
            for field_ in fields(entity):
                if 'index' in field_.metadata and field_.metadata['index'] is True:
                    self._cache['indexes'][entity].append(field_)

        return self._cache['indexes'][entity]

    def _add_index_params(self, entity: ff.Entity, params: list):
        for field_ in self._get_indexes(entity.__class__):
            params.append(self._generate_param_entry(field_.name, field_.type, getattr(entity, field_.name)))
        return params

    @staticmethod
    def _generate_param_entry(name: str, type_: str, val: any):
        t = 'stringValue'
        th = None
        if val is None:
            t = 'isNull'
            val = True
        elif type_ == 'float' or type_ is float:
            t = 'doubleValue'
        elif type_ == 'int' or type_ is int:
            t = 'longValue'
        elif type_ == 'bool' or type_ is bool:
            t = 'booleanValue'
        elif type_ == 'bytes' or type_ is bytes:
            t = 'blobValue'
        elif type_ == 'datetime' or type_ is datetime:
            val = str(val).replace('T', ' ')
            th = 'TIMESTAMP'
        ret = {'name': name, 'value': {t: val}}
        if th is not None:
            ret['typeHint'] = th
        return ret

    def _generate_index(self, name: str):
        return f'INDEX idx_{name} (`{name}`)'

    def _generate_extra(self, columns: list, indexes: list):
        return f", {','.join(columns)}, {','.join(indexes)}"

    def _ensure_connected(self):
        return True

    def _generate_where_clause(self, criteria: ff.BinaryOp):
        if criteria is None:
            return '', []

        clause, params = criteria.to_sql()
        ret = []
        for k, v in params.items():
            ret.append(self._generate_param_entry(k, type(v), v))
        return f'where {clause}', ret

    def _execute_ddl(self, entity: Type[ffd.Entity]):
        self._data_api.execute(f"create database if not exists {entity.get_class_context()}", [])
        self._data_api.execute(self._generate_create_table(entity), [])

        table_indexes = self._get_table_indexes(entity)
        indexes = self._get_indexes(entity)
        index_names = [f.name for f in indexes]

        for table_index in table_indexes:
            if table_index not in index_names:
                self._drop_table_index(entity, table_index)

        for index in index_names:
            if index not in table_indexes:
                self._add_table_index(entity, list(filter(lambda f: f.name == index, indexes))[0])

    @abstractmethod
    def _get_table_indexes(self, entity: Type[ffd.Entity]):
        pass

    @abstractmethod
    def _add_table_index(self, entity: Type[ffd.Entity], field_):
        pass

    @abstractmethod
    def _drop_table_index(self, entity: Type[ffd.Entity], name: str):
        pass

    def _get_result_count(self, sql: str, params: list):
        count_sql = f"select count(*) from ({sql}) a"
        result = ff.retry(lambda: self._data_api.execute(count_sql, params))
        return result['records'][0][0]['longValue']

    def _paginate(self, sql: str, params: list, entity: Type[ff.Entity], raw: bool = False):
        if entity.__name__ not in self._select_limits:
            self._select_limits[entity.__name__] = self._get_average_row_size(entity)
            if self._select_limits[entity.__name__] == 0:
                self._select_limits[entity.__name__] = 1
        limit = floor(self._size_limit / self._select_limits[entity.__name__])
        offset = 0

        ret = []
        while True:
            try:
                result = ff.retry(
                    lambda: self._data_api.execute(f'{sql} limit {limit} offset {offset}', params),
                    should_retry=lambda err: 'Database returned more than the allowed response size limit' not in str(err)
                )
            except ClientError as e:
                if 'Database returned more than the allowed response size limit' in str(e) and limit > 10:
                    limit = floor(limit / 2)
                    self._select_limits[entity.__name__] = limit
                    continue
                raise e

            for row in result['records']:
                ret.append(self._build_entity(entity, row, raw=raw))
            if len(result['records']) < limit:
                break
            offset += limit

        return ret

    def _load_query_results(self, sql: str, params: list, limit: int, offset: int):
        return ff.retry(
            lambda: self._data_api.execute(f'{sql} limit {limit} offset {offset}', params),
            should_retry=lambda err: 'Database returned more than the allowed response size limit'
                                     not in str(err)
        )['records']

    @abstractmethod
    def _get_average_row_size(self, entity: Type[ff.Entity]):
        """
        Retrieve the average row size in KB

        :param entity:
        :return:
        """
        pass
