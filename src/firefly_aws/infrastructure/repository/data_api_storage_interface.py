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

from abc import ABC
from dataclasses import fields
from datetime import datetime
from typing import Type

import firefly as ff
import firefly.infrastructure as ffi
import firefly_aws.domain as domain
from botocore.exceptions import ClientError
from firefly import domain as ffd


class DataApiStorageInterface(ffi.DbApiStorageInterface):
    _cache: dict = None
    _rds_data_client = None
    _serializer: ffi.JsonSerializer = None
    _db_arn: str = None
    _db_secret_arn: str = None
    _db_name: str = None
    _size_limit: int = 1000  # In KB

    def _disconnect(self):
        pass

    def _add(self, entity: ff.Entity):
        try:
            sql, params = self._generate_insert(entity)
            ff.retry(lambda: self._exec(sql, params))
        except domain.DocumentTooLarge:
            self._insert_large_document(entity)

    def _all(self, entity_type: Type[ff.Entity], criteria: ff.BinaryOp = None, limit: int = None):
        sql = f"select obj from {self._fqtn(entity_type)}"
        params = []
        if criteria is not None:
            clause, params = self._generate_where_clause(criteria)
            sql = f'{sql} {clause}'

        if limit is not None:
            sql += f" limit {limit}"

        try:
            result = ff.retry(
                lambda: self._exec(sql, params),
                should_retry=lambda err: 'Database returned more than the allowed response size limit' not in str(err)
            )

            ret = []
            for row in result['records']:
                obj = self._serializer.deserialize(row[0]['stringValue'])
                ret.append(entity_type.from_dict(obj))

            return ret
        except ClientError as e:
            if 'Database returned more than the allowed response size limit' in str(e):
                return self._fetch_multiple_large_documents(sql, params, entity_type)
            raise e

    def _find(self, uuid: str, entity_type: Type[ff.Entity]):
        sql = f"select obj from {self._fqtn(entity_type)} where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': uuid}},
        ]
        try:
            result = ff.retry(
                lambda: self._exec(sql, params),
                should_retry=lambda err: 'Database returned more than the allowed response size limit' not in str(err)
            )
            if len(result['records']) == 0:
                return None
            obj = self._serializer.deserialize(result['records'][0][0]['stringValue'])
            return entity_type.from_dict(obj)
        except ClientError as e:
            if 'Database returned more than the allowed response size limit' in str(e):
                return self._fetch_large_document(uuid, entity_type)
            raise e

    def _remove(self, entity: ff.Entity):
        sql = f"delete from {self._fqtn(entity.__class__)} where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
        ]
        ff.retry(self._exec(sql, params))

    def _update(self, entity: ff.Entity):
        try:
            sql, params = self._generate_update(entity)
            ff.retry(lambda: self._exec(sql, params))
        except domain.DocumentTooLarge:
            self._insert_large_document(entity, update=True)

    def _insert_large_document(self, entity: ff.Entity, update: bool = False):
        obj = self._serializer.serialize(entity)
        n = self._size_limit * 1024
        first = True
        for chunk in [obj[i:i+n] for i in range(0, len(obj), n)]:
            if first:
                if update:
                    ff.retry(lambda: self._exec(*self._generate_update(entity, part=chunk)))
                else:
                    ff.retry(lambda: self._exec(*self._generate_insert(entity, part=chunk)))
                first = False
            else:
                sql = f"update {self._fqtn(entity.__class__)} set obj = CONCAT(obj, :str) where id = :id"
                params = [
                    {'name': 'id', 'value': {'stringValue': entity.id_value()}},
                    {'name': 'str', 'value': {'stringValue': chunk}},
                ]
                ff.retry(lambda: self._exec(sql, params))

    def _fetch_large_document(self, id_: str, entity: Type[ff.Entity]):
        n = self._size_limit * 1024
        start = 1
        document = ''
        while True:
            sql = f"select SUBSTR(obj, {start}, {n}) as obj from {self._fqtn(entity)} where id = :id"
            params = [{'name': 'id', 'value': {'stringValue': id_}}]
            # result = ff.retry(self._exec(sql, params))
            result = self._exec(sql, params)
            document += result['records'][0][0]['stringValue']
            if len(result['records'][0][0]['stringValue']) < n:
                break
            start += n

        return entity.from_dict(self._serializer.deserialize(document))

    def _fetch_multiple_large_documents(self, sql: str, params: list, entity: Type[ff.Entity]):
        ret = []
        sql = sql.replace('select obj', 'select id')
        result = ff.retry(lambda: self._exec(sql, params))
        for row in result['records']:
            ret.append(self._fetch_large_document(row[0]['stringValue'], entity))
        return ret

    def _generate_insert(self, entity: ff.Entity, part: str = None):
        t = entity.__class__
        sql = f"insert into {self._fqtn(t)} ({self._generate_column_list(t)}) values ({self._generate_value_list(t)})"
        return sql, self._generate_parameters(entity, part=part)

    def _generate_update(self, entity: ff.Entity, part: str = None):
        t = entity.__class__
        sql = f"update {self._fqtn(t)} set {self._generate_update_list(t)} where id = :id"
        return sql, self._generate_parameters(entity, part=part)

    def _generate_where_clause(self, criteria: ff.BinaryOp):
        if criteria is None:
            return '', []

        clause, params = criteria.to_sql()
        ret = []
        for k, v in params.items():
            ret.append(self._generate_param_entry(k, type(v), v))
        return f'where {clause}', ret

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

    def _generate_parameters(self, entity: ff.Entity, part: str = None):
        if part is None:
            obj = self._serializer.serialize(entity)
            if (len(obj) / 1024) >= self._size_limit:
                raise domain.DocumentTooLarge()
        else:
            obj = part

        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
            {'name': 'obj', 'value': {'stringValue': obj}},
        ]
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
        self._exec(f"create database if not exists {entity.get_class_context()}", [])
        self._exec(self._generate_create_table(entity), [])
