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
from typing import Type, Union, Tuple

import firefly as ff
import firefly.infrastructure as ffi
from botocore.exceptions import ClientError
import firefly_aws.domain as domain
from firefly import domain as ffd
from firefly.infrastructure.repository.rdb_repository import Column

from firefly_aws.infrastructure.service.data_api import DataApi


class DataApiStorageInterface(ffi.RdbStorageInterface, ABC):
    _registry: ff.Registry = None
    _cache: dict = None
    _rds_data_client = None
    _serializer: ffi.JsonSerializer = None
    _data_api: DataApi = None
    _size_limit_kb: int = 1024
    _db_arn: str = None
    _db_secret_arn: str = None
    _db_name: str = None

    def __init__(self, db_arn: str = None, db_secret_arn: str = None, db_name: str = None, **kwargs):
        super().__init__(**kwargs)
        self._select_limits = {}

        if db_arn is not None:
            self._db_arn = db_arn
        if db_secret_arn is not None:
            self._db_secret_arn = db_secret_arn
        if db_name is not None:
            self._db_name = db_name

    def _disconnect(self):
        pass

    def _insert_large_document(self, entity: ff.Entity, update: bool = False):
        obj = self._serialize_entity(entity)
        n = self._size_limit_kb * 1024
        first = True
        for chunk in [obj[i:i+n] for i in range(0, len(obj), n)]:
            if first:
                if update:
                    self._execute(*self._generate_query(entity, f'{self._sql_prefix}/update.sql', {
                        'data': {'document': chunk},
                        'criteria': ffd.Attr(entity.id_name()) == entity.id_value(),
                    }))
                else:
                    self._execute(*self._generate_query(entity, f'{self._sql_prefix}/insert.sql', {
                        'data': {
                            entity.id_name(): entity.id_value(),
                            'document': chunk
                        },
                        'criteria': ffd.Attr(entity.id_name()) == entity.id_value(),
                    }))
                first = False
            else:
                sql = f"update {self._fqtn(entity.__class__)} set document = CONCAT(obj, :str) where id = :id"
                params = [
                    {'name': 'id', 'value': {'stringValue': entity.id_value()}},
                    {'name': 'str', 'value': {'stringValue': chunk}},
                ]
                self._execute(sql, params)

    def _fetch_multiple_large_documents(self, sql: str, params: list, entity: Type[ff.Entity]):
        ret = []
        sql = sql.replace('select obj', 'select id')
        result = ff.retry(lambda: self._execute(sql, params))
        for row in result['records']:
            ret.append(self._fetch_large_document(row[0]['stringValue'], entity))
        return ret

    def _fetch_large_document(self, id_: str, entity: Type[ff.Entity]):
        n = self._size_limit_kb * 1024
        start = 1
        document = ''
        while True:
            sql, params = self._generate_query(entity, f'{self._sql_prefix}/select.sql', {
                'columns': [f'SUBSTR(obj, {start}, {n}) as obj'],
            })
            result = self._execute(sql, params)
            document += result['records'][0][0]['stringValue']
            if len(result['records'][0][0]['stringValue']) < n:
                break
            start += n

        return entity.from_dict(self._serializer.deserialize(document))

    def _ensure_connected(self):
        return True

    def _get_result_count(self, sql: str, params: list):
        count_sql = f"select count(*) from ({sql}) a"
        result = ff.retry(lambda: self._execute(count_sql, params))
        return result['records'][0][0]['longValue']

    def _paginate(self, sql: str, params: list, entity: Type[ff.Entity], raw: bool = False):
        if entity.__name__ not in self._select_limits:
            self._select_limits[entity.__name__] = self._get_average_row_size(entity)
            if self._select_limits[entity.__name__] == 0:
                self._select_limits[entity.__name__] = 1
        limit = floor(self._size_limit_kb / self._select_limits[entity.__name__])
        offset = 0

        ret = []
        while True:
            try:
                result = ff.retry(
                    lambda: self._execute(f'{sql} limit {limit} offset {offset}', params),
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
            lambda: self._execute(f'{sql} limit {limit} offset {offset}', params),
            should_retry=lambda err: 'Database returned more than the allowed response size limit'
                                     not in str(err)
        )['records']

    def _get_table_columns(self, entity: Type[ffd.Entity]):
        result = self._execute(*self._generate_query(entity, 'mysql/get_columns.sql'))
        ret = []
        if result:
            for row in result:
                ret.append(Column(name=row['COLUMN_NAME'], type=row['COLUMN_TYPE']))
        return ret

    def _build_entity(self, entity: Type[ffd.Entity], data, raw: bool = False):
        if raw is True:
            return self._serializer.deserialize(data['document'])
        data = self._serializer.deserialize(data['document'])
        for k, v in self._get_relationships(entity).items():
            if v['this_side'] == 'one':
                data[k] = self._registry(v['target']).find(data[k])
            elif v['this_side'] == 'many':
                data[k] = self._registry(v['target']).filter(
                    lambda ee: getattr(ee, v['target'].id_name()).is_in(data[k])
                )
        return entity.from_dict(data)

    def _get_average_row_size(self, entity: Type[ff.Entity]):
        result = self._execute(f"select CEIL(AVG(LENGTH(obj))) from {self._fqtn(entity)}")
        try:
            return result['records'][0][0]['longValue'] / 1024
        except KeyError:
            return 1

    @staticmethod
    def _generate_param_entry(name: str, type_: str, val: any):
        t = 'stringValue'
        th = None
        if val is None:
            t = 'isNull'
            val = True
        elif type_ == 'float' or type_ is float:
            val = float(val)
            t = 'doubleValue'
        elif type_ == 'int' or type_ is int:
            val = int(val)
            t = 'longValue'
        elif type_ == 'bool' or type_ is bool:
            val = bool(val)
            t = 'booleanValue'
        elif type_ == 'bytes' or type_ is bytes:
            t = 'blobValue'
        elif type_ == 'datetime' or type_ is datetime:
            val = str(val).replace('T', ' ')
            th = 'TIMESTAMP'
        else:
            val = str(val)

        ret = {'name': name, 'value': {t: val}}
        if th is not None:
            ret['typeHint'] = th
        return ret

    def _execute(self, sql: str, params: Union[dict, list] = None):
        if isinstance(params, dict):
            converted = []
            for k, v in params.items():
                converted.append(self._generate_param_entry(k, type(v), v))
            params = converted

        # return ff.retry(
        #     lambda: self._data_api.execute(sql, params),
        #     should_retry=lambda err: 'Database returned more than the allowed response size limit' not in str(err)
        # )
        result = self._data_api.execute(
            sql,
            params,
            db_arn=self._db_arn,
            db_secret_arn=self._db_secret_arn,
            db_name=self._db_name
        )

        if 'records' in result:
            ret = []
            for row in result['records']:
                counter = 0
                d = {}
                for data in result['columnMetadata']:
                    d[data['name']] = list(row[counter].values())[0]
                    counter += 1
                ret.append(d)
            return ret
        else:
            return result
