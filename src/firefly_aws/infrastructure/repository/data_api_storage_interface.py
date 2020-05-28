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


class DataApiStorageInterface(ABC):
    _rds_data_client = None
    _serializer: ffi.JsonSerializer = None
    _db_arn: str = None
    _db_secret_arn: str = None
    _db_name: str = None
    _cache: dict = None

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

    @staticmethod
    def _generate_param_entry(name: str, type_: str, val: any):
        t = 'stringValue'
        if type_ == 'float' or type_ is float:
            t = 'doubleValue'
        elif type_ == 'int' or type_ is int:
            t = 'longValue'
        elif type_ == 'bool' or type_ is bool:
            t = 'booleanValue'
        elif type_ == 'bytes' or type_ is bytes:
            t = 'blobValue'
        elif type_ == 'datetime' or type_ is datetime:
            val = str(val)
        return {'name': name, 'value': {t: val}}
