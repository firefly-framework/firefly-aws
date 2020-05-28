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
from firefly import domain as ffd


class DataApiMysqlStorageInterface(ffi.DbApiStorageInterface):
    _rds_data_client = None
    _serializer: ffi.JsonSerializer = None
    _db_arn: str = None
    _db_secret_arn: str = None
    _db_name: str = None

    def __init__(self):
        super().__init__()
        self._cache = {
            'sql': {},
            'indexes': {},
        }

    def _disconnect(self):
        pass

    def _add(self, entity: ffd.Entity):
        entity_type = entity.__class__
        key = entity_type.__name__
        if key not in self._cache['sql']:
            cols = [f.name for f in self._get_indexes(entity_type)]
            placeholders = [f':{f.name}' for f in self._get_indexes(entity_type)]

            if len(cols) > 0:
                cols = f",{','.join(cols)}"
                placeholders = f",{','.join(placeholders)}"
            else:
                cols = ''
                placeholders = ''
            self._cache['sql'][key] = f"""
                insert into {self._fqtn(entity.__class__)} (id, obj{cols}) 
                values (:id, :obj{placeholders})
            """

        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
            {'name': 'obj', 'value': {'stringValue': self._serializer.serialize(entity)}},
        ]
        for field_ in self._get_indexes(entity_type):
            t = 'stringValue'
            val = getattr(entity, field_.name)
            if field_.type == 'float':
                t = 'doubleValue'
            elif field_.type == 'int':
                t = 'longValue'
            elif field_.type == 'bool':
                t = 'booleanValue'
            elif field_.type == 'bytes':
                t = 'blobValue'
            elif field_.type == 'datetime':
                val = str(val)
            params.append({'name': field_.name, 'value': {t: val}})

        ff.retry(lambda: self._exec(self._cache['sql'][key], params))

    def _all(self, entity_type: Type[ffd.Entity], criteria: ffd.BinaryOp = None, limit: int = None):
        sql = f"select obj from {self._fqtn(entity_type)}"
        if limit is not None:
            sql += f" limit {limit}"
        result = ff.retry(lambda: self._exec(sql, []))

        ret = []
        for row in result['records']:
            obj = self._serializer.deserialize(row[0])
            ret.append(entity_type.from_dict(obj))

        return ret

    def _find(self, uuid: str, entity_type: Type[ffd.Entity]):
        sql = f"select obj from {self._fqtn(entity_type)} where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': uuid}},
        ]
        result = ff.retry(lambda: self._exec(sql, params))
        if len(result['records']) == 0:
            return None
        obj = self._serializer.deserialize(result['records'][0][0]['stringValue'])
        return entity_type.from_dict(obj)

    def _remove(self, entity: ffd.Entity):
        sql = f"delete from {self._fqtn(entity.__class__)} where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
        ]
        ff.retry(self._exec(sql, params))

    def _update(self, entity: ffd.Entity):
        sql = f"update {self._fqtn(entity.__class__)} set obj = :obj where id = :id"
        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
            {'name': 'obj', 'value': {'stringValue': self._serializer.serialize(entity)}},
        ]
        ff.retry(self._exec(sql, params))

    def _ensure_connected(self):
        return True

    def _ensure_table_created(self, entity: Type[ffd.Entity]):
        columns = []
        indexes = []
        for i in self._get_indexes(entity):
            indexes.append(f'INDEX idx_{i.name} (`{i.name}`)')
            if i.type == 'float':
                columns.append(f"`{i.name}` float")
            elif i.type == 'int':
                columns.append(f"`{i.name}` integer")
            elif i.type == 'datetime':
                columns.append(f"`{i.name}` datetime")
            else:
                length = i.metadata['length'] if 'length' in i.metadata else 256
                columns.append(f"`{i.name}` varchar({length})")
        extra = ''
        if len(columns) > 0:
            extra = f", {','.join(columns)}, {','.join(indexes)}"

        sql = f"""
            create table if not exists {self._fqtn(entity)} (
                id varchar(36)
                , obj longtext not null
                {extra}
                , primary key(id)
            )
        """
        self._exec(sql, [])

    def _get_indexes(self, entity: Type[ffd.Entity]):
        if entity not in self._cache['indexes']:
            self._cache['indexes'][entity] = []
            for field_ in fields(entity):
                if 'index' in field_.metadata and field_.metadata['index'] is True:
                    self._cache['indexes'][entity].append(field_)

        return self._cache['indexes'][entity]

    @staticmethod
    def _fqtn(entity: Type[ffd.Entity]):
        return inflection.tableize(entity.__name__)

    def _exec(self, sql: str, params: list):
        return self._rds_data_client.execute_statement(
            resourceArn=self._db_arn,
            secretArn=self._db_secret_arn,
            database=self._db_name,
            sql=sql,
            parameters=params
        )
