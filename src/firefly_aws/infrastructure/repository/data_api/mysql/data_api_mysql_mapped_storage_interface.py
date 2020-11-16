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

import inspect
from dataclasses import fields
from typing import Type, get_type_hints, List, Dict

import firefly as ff
from firefly import domain as ffd

from .data_api_mysql_base import DataApiMysqlBase


# noinspection PyDataclass
class DataApiMysqlMappedStorageInterface(DataApiMysqlBase):
    _sql_prefix = 'mysql'
    _identifier_quote_char = '`'
    _map_all = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_average_row_size(self, entity: Type[ff.Entity]):
        schema, table = self._fqtn(entity).split('.')
        sql = f"""
            select avg_row_length
            from information_schema.tables
            where table_schema = '{schema}'
            and table_name = '{table}'
        """
        result = ff.retry(lambda: self._execute(sql))
        try:
            return result[0]['AVG_ROW_LENGTH'] / 1024
        except KeyError:
            return 1
