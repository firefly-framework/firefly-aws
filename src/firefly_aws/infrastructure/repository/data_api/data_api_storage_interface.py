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
from datetime import datetime
from math import floor
from typing import Type, Union, Callable, Tuple, List

import firefly as ff
import firefly.infrastructure as ffi
from botocore.exceptions import ClientError
from firefly import domain as ffd

import firefly_aws.domain as domain
from firefly_aws.infrastructure.service.data_api import DataApi


class DataApiStorageInterface(ffi.RdbStorageInterface, ABC):
    _cache: dict = None
    _rds_data_client = None
    _serializer: ffi.JsonSerializer = None
    _data_api: DataApi = None
    _size_limit_kb: int = 1000
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

    def _add(self, entity: List[ffd.Entity]):
        try:
            return super()._add(entity)
        except domain.DocumentTooLarge:
            for e in entity:
                self._insert_large_document(e)

    def _all(self, entity_type: Type[ffd.Entity], criteria: ffd.BinaryOp = None, limit: int = None, offset: int = None,
             sort: Tuple[Union[str, Tuple[str, bool]]] = None, raw: bool = False, count: bool = False):
        try:
            return super()._all(
                entity_type, criteria, limit=limit, offset=offset, sort=sort, raw=raw, count=count
            )
        except domain.DocumentTooLarge:
            query = self._generate_select(
                entity_type, criteria, limit=limit, offset=offset, sort=sort, count=count
            )
            try:
                return self._paginate(query[0], query[1], entity_type, raw=raw)
            except domain.DocumentTooLarge:
                return self._fetch_multiple_large_documents(query[0], query[1], entity_type)

    def _find(self, uuid: Union[str, Callable], entity_type: Type[ffd.Entity]):
        try:
            return super()._find(uuid, entity_type)
        except domain.DocumentTooLarge:
            return self._fetch_large_document(uuid, entity_type)

    def _remove(self, entity: ffd.Entity):
        return super()._remove(entity)

    def _update(self, entity: ffd.Entity):
        try:
            return super()._update(entity)
        except domain.DocumentTooLarge:
            self._insert_large_document(entity, update=True)

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
                        'data': {'__document': ''},
                        'criteria': ffd.Attr(entity.id_name()) == entity.id_value(),
                    }))
                    data = self._data_fields(entity)
                    del data['document']
                    data['__document'] = chunk
                    self._execute(*self._generate_query(entity, f'{self._sql_prefix}/update.sql', {
                        'data': data,
                        'criteria': ffd.Attr(entity.id_name()) == entity.id_value(),
                    }))
                else:
                    data = self._data_fields(entity)
                    del data['document']
                    data['__document'] = chunk
                    data[entity.id_name()] = entity.id_value()
                    self._execute(*self._generate_query(entity, f'{self._sql_prefix}/insert.sql', {
                        'data': [data],
                        'criteria': ffd.Attr(entity.id_name()) == entity.id_value(),
                    }))
                first = False
            else:
                sql = f"update {self._fqtn(entity.__class__)} set __document = CONCAT(__document, :str) " \
                      f"where id = :id{self._cast_uuid()}"
                params = {'id': entity.id_value(), 'str': chunk}
                self._execute(sql, params)

        sql = f"update {self._fqtn(entity.__class__)} set document = __document{self._cast_json()} " \
              f"where id = :id{self._cast_uuid()}"
        params = {'id': entity.id_value()}
        self._execute(sql, params)

        self._execute(*self._generate_query(entity, f'{self._sql_prefix}/update.sql', {
            'data': {'__document': ''},
            'criteria': ffd.Attr(entity.id_name()) == entity.id_value(),
        }))

    @staticmethod
    def _cast_json():
        return ''

    @staticmethod
    def _cast_uuid():
        return ''

    def _fetch_multiple_large_documents(self, sql: str, params: list, entity: Type[ff.Entity]):
        ret = []
        q = self._identifier_quote_char
        sql = sql.replace(f'select {q}document{q}', 'select id')
        result = ff.retry(lambda: self._execute(sql, params))
        for row in result:
            ret.append(self._fetch_large_document(row['id'], entity))
        return ret

    def _fetch_large_document(self, id_: str, entity: Type[ff.Entity]):
        n = self._size_limit_kb * 1024
        start = 1
        document = ''
        self._execute(f"update {self._fqtn(entity)} set __document = document where {entity.id_name()} = '{id_}'")
        while True:
            sql, params = self._generate_query(entity, f'{self._sql_prefix}/select.sql', {
                'columns': [self._substr(start, n)],
                'criteria': ffd.Attr(entity.id_name()) == id_
            })
            result = self._execute(sql, params)
            document += result[0]['document']
            if len(result[0]['document']) < n:
                break
            start += n

        return entity.from_dict(self._serializer.deserialize(document))

    @staticmethod
    def _substr(start: int, n: int):
        return f'SUBSTR(__document, {start}, {n}) as document'

    def _ensure_connected(self):
        return True

    def _get_result_count(self, sql: str, params: list):
        count_sql = f"select count(1) as c from ({sql}) a"
        result = ff.retry(lambda: self._execute(count_sql, params))
        return result[0]['c']

    def _paginate(self, sql: str, params: list, entity: Type[ff.Entity], raw: bool = False):
        if entity.__name__ not in self._select_limits:
            self._select_limits[entity.__name__] = self._get_average_row_size(entity)
            if self._select_limits[entity.__name__] == 0:
                self._select_limits[entity.__name__] = 1
        limit = floor(self._size_limit_kb / self._select_limits[entity.__name__])
        if limit == 0:
            raise domain.DocumentTooLarge()
        offset = 0

        ret = []
        while True:
            try:
                result = ff.retry(
                    lambda: self._execute(f'{sql} limit {limit} offset {offset}', params),
                    should_retry=lambda err: 'Database returned more than the allowed response size limit' not in
                                             str(err) and '(413)' not in str(err)
                )
            except domain.DocumentTooLarge:
                if limit > 10:
                    limit = floor(limit / 2)
                    self._select_limits[entity.__name__] = limit
                    continue
                raise

            for row in result:
                ret.append(self._build_entity(entity, row, raw=raw))
            if len(result) < limit:
                break
            offset += limit

        return ret

    def _load_query_results(self, sql: str, params: list, limit: int, offset: int):
        return ff.retry(
            lambda: self._execute(f'{sql} limit {limit} offset {offset}', params),
            should_retry=lambda err: 'Database returned more than the allowed response size limit'
                                     not in str(err) and '(413)' not in str(err)
        )

    def _get_average_row_size(self, entity: Type[ff.Entity]):
        result = self._execute(f"select CEIL(AVG(LENGTH(document))) as c from {self._fqtn(entity)}")
        try:
            return result[0]['c'] / 1024
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

        # result = ff.retry(
        #     lambda: self._data_api.execute(
        #         sql,
        #         params,
        #         db_arn=self._db_arn,
        #         db_secret_arn=self._db_secret_arn,
        #         db_name=self._db_name
        #     ),
        #     should_retry=lambda err: 'Database returned more than the allowed response size limit' not in str(err) and '(413)' not in str(err)
        # )
        try:
            result = self._data_api.execute(
                sql,
                params,
                db_arn=self._db_arn,
                db_secret_arn=self._db_secret_arn,
                db_name=self._db_name
            )
        except ClientError as e:
            if 'Database returned more than the allowed response size limit' in str(e) or '(413)' in str(e):
                raise domain.DocumentTooLarge()
            raise e

        if 'records' in result:
            ret = []
            for row in result['records']:
                counter = 0
                d = {}

                for data in result['columnMetadata']:
                    if 'isNull' in row[counter] and row[counter]['isNull']:
                        d[data['name']] = None
                    else:
                        d[data['name']] = list(row[counter].values())[0]
                    counter += 1
                ret.append(d)
            return ret
        else:
            return result['numberOfRecordsUpdated']
