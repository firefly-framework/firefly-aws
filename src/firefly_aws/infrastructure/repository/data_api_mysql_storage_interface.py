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

from dataclasses import fields
from datetime import datetime
from typing import Type

import firefly as ff
import firefly.infrastructure as ffi
import inflection


class DataApiMysqlStorageInterface(ffi.DbApiStorageInterface):
    _rds_data_client = None
    _serializer: ffi.JsonSerializer = None
    _db_arn: str = None
    _db_secret_arn: str = None
    _db_name: str = None

    def __init__(self):
        super().__init__()

    def _disconnect(self):
        pass

    def _add(self, entity: ff.Entity):
        sql, params = self._generate_insert(entity)
        ff.retry(lambda: self._exec(sql, params))

    def _all(self, entity_type: Type[ff.Entity], criteria: ff.BinaryOp = None, limit: int = None):
        sql = f"select obj from {self._fqtn(entity_type)}"
        params = []
        if criteria is not None:
            clause, params = self._generate_where_clause(criteria)
            sql = f'{sql} where {clause}'

        if limit is not None:
            sql += f" limit {limit}"
        result = ff.retry(lambda: self._exec(sql, params))

        ret = []
        for row in result['records']:
            obj = self._serializer.deserialize(row[0]['stringValue'])
            ret.append(entity_type.from_dict(obj))

        return ret

    def _find(self, uuid: str, entity_type: Type[ff.Entity]):
        sql = f"select obj from {self._fqtn(entity_type)} where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': uuid}},
        ]
        result = ff.retry(lambda: self._exec(sql, params))
        if len(result['records']) == 0:
            return None
        obj = self._serializer.deserialize(result['records'][0][0]['stringValue'])
        return entity_type.from_dict(obj)

    def _remove(self, entity: ff.Entity):
        sql = f"delete from {self._fqtn(entity.__class__)} where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
        ]
        ff.retry(self._exec(sql, params))

    def _update(self, entity: ff.Entity):
        sql, params = self._generate_update(entity)
        ff.retry(lambda: self._exec(sql, params))

    def _ensure_connected(self):
        return True

    def _ensure_table_created(self, entity: Type[ff.Entity]):
        self._exec(self._generate_create_table(entity), [])

    def _exec(self, sql: str, params: list):
        return self._rds_data_client.execute_statement(
            resourceArn=self._db_arn,
            secretArn=self._db_secret_arn,
            database=self._db_name,
            sql=sql,
            parameters=params
        )

    def _generate_where_clause(self, criteria: ff.BinaryOp):
        if criteria is None:
            return '', []

        clause, params = criteria.to_sql()
        ret = []
        for k, v in params.items():
            ret.append(self._generate_param_entry(k, type(v), v))
        return f'where {clause}', ret

    def _add_index_params(self, entity: ff.Entity, params: list):
        for field_ in self._get_indexes(entity.__class__):
            params.append(self._generate_param_entry(field_.name, field_.type, getattr(entity, field_.name)))
        return params

    @staticmethod
    def _generate_param_entry(name: str, type_: str, val: any):
        t = 'stringValue'
        th = None
        if type_ == 'float' or type_ is float:
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

    def _generate_parameters(self, entity: ff.Entity):
        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
            {'name': 'obj', 'value': {'stringValue': self._serializer.serialize(entity)}},
        ]
        for field_ in self._get_indexes(entity.__class__):
            params.append(self._generate_param_entry(field_.name, field_.type, getattr(entity, field_.name)))
        return params

    def _generate_index(self, name: str):
        return f'INDEX idx_{name} (`{name}`)'

    def _generate_extra(self, columns: list, indexes: list):
        return f", {','.join(columns)}, {','.join(indexes)}"
