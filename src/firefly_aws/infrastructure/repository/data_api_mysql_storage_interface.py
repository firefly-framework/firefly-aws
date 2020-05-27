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

    def _disconnect(self):
        pass

    def _add(self, entity: ffd.Entity):
        sql = f"insert into {self._fqtn(entity.__class__)} (id, obj) values (:id, :obj)"
        params = [
            {'name': 'id', 'value': {'stringValue': entity.id_value()}},
            {'name': 'obj', 'value': {'stringValue': self._serializer.serialize(entity)}},
        ]
        ff.retry(lambda: self._exec(sql, params))

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
        sql = f"""
            create table if not exists {self._fqtn(entity)} (
                id varchar(36) primary key,
                obj longtext not null
            )
        """
        self._exec(sql, [])

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
