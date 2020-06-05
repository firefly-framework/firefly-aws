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

from math import ceil
from typing import Type

import firefly as ff
from firefly import domain as ffd

from .data_api_storage_interface import DataApiStorageInterface


class DataApiMysqlStorageInterface(DataApiStorageInterface):
    def __init__(self):
        super().__init__()

    def _get_average_row_size(self, entity: Type[ff.Entity]):
        result = ff.retry(
            lambda: self._exec(f"select CEIL(AVG(LENGTH(obj))) from {self._fqtn(entity)}", [])
        )
        try:
            return result['records'][0][0]['longValue'] / 1024
        except KeyError:
            return 1

    def _get_table_indexes(self, entity: Type[ffd.Entity]):
        schema, table = self._fqtn(entity).split('.')
        sql = f"""
            select COLUMN_NAME
            from information_schema.STATISTICS
            where TABLE_NAME = '{table}'
            and TABLE_SCHEMA = '{schema}'
            and INDEX_NAME != 'PRIMARY'
        """
        result = ff.retry(
            lambda: self._exec(sql, [])
        )

        ret = []
        for row in result['records']:
            ret.append(row[0]['stringValue'])

        return ret

    def _add_table_index(self, entity: Type[ffd.Entity], field_):
        ff.retry(lambda: self._exec(
            f"alter table {self._fqtn(entity)} add column `{field_.name}` {self._db_type(field_)}", []
        ))
        ff.retry(lambda: self._exec(f"create index `idx_{field_.name}` on {self._fqtn(entity)} (`{field_.name}`)", []))

    def _drop_table_index(self, entity: Type[ffd.Entity], name: str):
        ff.retry(lambda: self._exec(f"drop index `idx_{name}` on {self._fqtn(entity)}", []))
        ff.retry(lambda: self._exec(f"alter table {self._fqtn(entity)} drop column `{name}`", []))
