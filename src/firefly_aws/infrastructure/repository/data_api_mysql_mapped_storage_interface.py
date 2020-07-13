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
from typing import Type

import firefly as ff
from firefly import domain as ffd

from .data_api_storage_interface import DataApiStorageInterface


# noinspection PyDataclass
class DataApiMysqlMappedStorageInterface(DataApiStorageInterface):
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
        result = ff.retry(lambda: self._data_api.execute(sql, []))
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
            lambda: self._data_api.execute(sql, [])
        )

        ret = []
        for row in result['records']:
            ret.append(row[0]['stringValue'])

        return ret

    def _add_table_index(self, entity: Type[ffd.Entity], field_):
        ff.retry(lambda: self._data_api.execute(
            f"alter table {self._fqtn(entity)} add column `{field_.name}` {self._db_type(field_)}", []
        ))
        name = f'`{field_.name}`'
        ff.retry(lambda: self._data_api.execute(f"create index `idx_{field_.name}` on {self._fqtn(entity)} ({name})", []))

    def _drop_table_index(self, entity: Type[ffd.Entity], name: str):
        index = f'`idx_{name}`'
        ff.retry(lambda: self._data_api.execute(f"drop index {index} on {self._fqtn(entity)}", []))
        column = f'`{name}`'
        ff.retry(lambda: self._data_api.execute(f"alter table {self._fqtn(entity)} drop column {column}", []))

    def _generate_column_list(self, entity: Type[ffd.Entity]):
        if entity not in self._cache['parts']['columns']:
            columns = list(map(lambda f: f'`{f.name}`', self._visible_fields(entity)))
            self._cache['parts']['columns'][entity] = ','.join(columns)

        return self._cache['parts']['columns'][entity]

    def _generate_value_list(self, entity: Type[ffd.Entity]):
        if entity not in self._cache['parts']['values']:
            placeholders = list(map(lambda f: f':{f.name}', self._visible_fields(entity)))
            self._cache['parts']['values'][entity] = ','.join(placeholders)

        return self._cache['parts']['values'][entity]

    def _generate_parameters(self, entity: ff.Entity, part: str = None):
        params = []
        for field_ in fields(entity):
            if 'hidden' in field_.metadata:
                continue
            params.append(self._generate_param_entry(field_.name, field_.type, getattr(entity, field_.name)))
        return params

    def _generate_param_entry(self, name: str, type_: str, val: any):
        if type_ == 'dict' or type_ is dict:
            return {'name': name, 'value': {'stringValue': self._serializer.serialize(val)}}

        return super()._generate_param_entry(name, type_, val)

    def _generate_update_list(self, entity: Type[ffd.Entity]):
        return ','.join(list(map(lambda f: f'`{f.name}`=:{f.name}', self._visible_fields(entity))))

    def _generate_select_list(self, entity: Type[ffd.Entity]):
        if entity not in self._cache['parts']['select']:
            fields_ = list(map(lambda f: f'`{f.name}`', self._visible_fields(entity)))
            self._cache['parts']['select'][entity] = ','.join(fields_)
        return self._cache['parts']['select'][entity]

    def _build_entity(self, entity: Type[ffd.Entity], data, raw: bool = False):
        params = {}

        for i, field_ in enumerate(self._visible_fields(entity)):
            type_ = field_.type
            if 'isNull' in data[i]:
                params[field_.name] = None
                continue
            elif type_ == 'float' or type_ is float:
                t = 'doubleValue'
            elif type_ == 'int' or type_ is int:
                t = 'longValue'
            elif type_ == 'bool' or type_ is bool:
                t = 'booleanValue'
            elif type_ == 'bytes' or type_ is bytes:
                t = 'blobValue'
            else:
                t = 'stringValue'

            params[field_.name] = data[i][t]

        if raw:
            return params

        return entity.from_dict(params)

    @staticmethod
    def _visible_fields(entity: Type[ffd.Entity]):
        return list(filter(lambda f: 'hidden' not in f.metadata, fields(entity)))

    def _generate_create_table(self, entity: Type[ffd.Entity]):
        columns = []
        indexes = []
        for i in fields(entity):
            if 'hidden' in i.metadata:
                continue
            if 'index' in i.metadata and i.metadata['index'] is True:
                indexes.append(self._generate_index(i.name))
            if i.type == 'float':
                columns.append(f"`{i.name}` float")
            elif i.type == 'int':
                columns.append(f"`{i.name}` integer")
            elif i.type == 'datetime':
                columns.append(self._datetime_declaration(i.name))
            elif i.type == 'bool':
                columns.append(f"`{i.name}` boolean")
            elif 'length' in i.metadata:
                columns.append(f"`{i.name}` varchar({i.metadata['length']})")
            else:
                columns.append(f"`{i.name}` text")

        index_code = ''
        if len(indexes) > 0:
            index_code = ',' + ','.join(indexes)

        sql = f"""
            create table if not exists {self._fqtn(entity)} (
                {','.join(columns)}
                {index_code}
                , primary key(`{entity.id_name()}`)
            )
        """
        return sql

    def raw(self, entity: Type[ffd.Entity], criteria: ffd.BinaryOp = None, limit: int = None):
        sql = f"select {self._generate_select_list(entity)} from {self._fqtn(entity)}"
        params = []
        if criteria is not None:
            clause, params = self._generate_where_clause(criteria)
            sql = f'{sql} {clause}'

        if limit is not None:
            sql += f" limit {limit}"

        return self._paginate(sql, params, entity, raw=True)
