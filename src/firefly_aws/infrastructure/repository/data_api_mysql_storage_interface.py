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
import firefly_aws.domain as domain
from botocore.exceptions import ClientError
from firefly import domain as ffd

from .data_api_storage_interface import DataApiStorageInterface


class DataApiMysqlStorageInterface(DataApiStorageInterface):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _add(self, entity: ff.Entity):
        try:
            super()._add(entity)
        except domain.DocumentTooLarge:
            self._insert_large_document(entity)

    def _update(self, entity: ff.Entity):
        try:
            super()._update(entity)
        except domain.DocumentTooLarge:
            self._insert_large_document(entity, update=True)

    def _find(self, uuid: str, entity_type: Type[ff.Entity]):
        try:
            return super()._find(uuid, entity_type)
        except ClientError as e:
            if 'Database returned more than the allowed response size limit' in str(e):
                return self._fetch_large_document(uuid, entity_type)
            raise e

    def _all(self, entity_type: Type[ff.Entity], criteria: ff.BinaryOp = None, limit: int = None):
        try:
            indexes = [i.name for i in self.get_indexes(entity_type)]
            pruned_criteria = None
            if criteria is not None:
                pruned_criteria = criteria.prune(indexes)

            entities = super()._all(entity_type, pruned_criteria, limit)
            if criteria != pruned_criteria:
                entities = list(filter(lambda ee: criteria.matches(ee), entities))

            return entities
        except ClientError as e:
            if 'Database returned more than the allowed response size limit' in str(e):
                sql = f"select {self._generate_select_list(entity_type)} from {self._fqtn(entity_type)}"
                params = []
                if criteria is not None:
                    clause, params = self._generate_where_clause(criteria)
                    sql = f'{sql} {clause}'

                if limit is not None:
                    sql += f" limit {limit}"

                return self._fetch_multiple_large_documents(sql, params, entity_type)
            raise e

    def _get_average_row_size(self, entity: Type[ff.Entity]):
        result = ff.retry(
            lambda: self._data_api.execute(f"select CEIL(AVG(LENGTH(obj))) from {self._fqtn(entity)}", [])
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
        ff.retry(lambda: self._data_api.execute(f"create index `idx_{field_.name}` on {self._fqtn(entity)} (`{field_.name}`)", []))

    def _drop_table_index(self, entity: Type[ffd.Entity], name: str):
        ff.retry(lambda: self._data_api.execute(f"drop index `idx_{name}` on {self._fqtn(entity)}", []))
        ff.retry(lambda: self._data_api.execute(f"alter table {self._fqtn(entity)} drop column `{name}`", []))

    def _generate_column_list(self, entity: Type[ffd.Entity]):
        values = ['id', 'obj']
        for index in self.get_indexes(entity):
            values.append(index.name)
        return ','.join(values)

    def _generate_value_list(self, entity: Type[ffd.Entity]):
        placeholders = [':id', ':obj']
        for index in self.get_indexes(entity):
            placeholders.append(f':{index.name}')
        return ','.join(placeholders)

    def _generate_parameters(self, entity: ff.Entity, part: str = None):
        if part is None:
            obj = self._serializer.serialize(entity.to_dict(force_all=True))
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

    def _generate_update_list(self, entity: Type[ffd.Entity]):
        values = ['obj=:obj']
        for index in self.get_indexes(entity):
            values.append(f'`{index.name}`=:{index.name}')
        return ','.join(values)

    def _insert_large_document(self, entity: ff.Entity, update: bool = False):
        obj = self._serializer.serialize(entity.to_dict(force_all=True))
        n = self._size_limit * 1024
        first = True
        for chunk in [obj[i:i+n] for i in range(0, len(obj), n)]:
            if first:
                if update:
                    ff.retry(lambda: self._data_api.execute(*self._generate_update(entity, part=chunk)))
                else:
                    ff.retry(lambda: self._data_api.execute(*self._generate_insert(entity, part=chunk)))
                first = False
            else:
                sql = f"update {self._fqtn(entity.__class__)} set obj = CONCAT(obj, :str) where id = :id"
                params = [
                    {'name': 'id', 'value': {'stringValue': entity.id_value()}},
                    {'name': 'str', 'value': {'stringValue': chunk}},
                ]
                ff.retry(lambda: self._data_api.execute(sql, params))

    def _fetch_large_document(self, id_: str, entity: Type[ff.Entity]):
        n = self._size_limit * 1024
        start = 1
        document = ''
        while True:
            sql = f"select SUBSTR(obj, {start}, {n}) as obj from {self._fqtn(entity)} where id = :id"
            params = [{'name': 'id', 'value': {'stringValue': id_}}]
            # result = ff.retry(self._data_api.execute(sql, params))
            result = self._data_api.execute(sql, params)
            document += result['records'][0][0]['stringValue']
            if len(result['records'][0][0]['stringValue']) < n:
                break
            start += n

        return entity.from_dict(self._serializer.deserialize(document))

    def _fetch_multiple_large_documents(self, sql: str, params: list, entity: Type[ff.Entity]):
        ret = []
        sql = sql.replace('select obj', 'select id')
        result = ff.retry(lambda: self._data_api.execute(sql, params))
        for row in result['records']:
            ret.append(self._fetch_large_document(row[0]['stringValue'], entity))
        return ret

    def _build_entity(self, entity: Type[ffd.Entity], data, raw: bool = False):
        if raw is True:
            return self._serializer.deserialize(data[0]['stringValue'])
        return entity.from_dict(self._serializer.deserialize(data[0]['stringValue']))

    def _generate_select_list(self, entity: Type[ffd.Entity]):
        return 'obj'

    def _generate_create_table(self, entity: Type[ffd.Entity]):
        columns = []
        indexes = []
        for i in self.get_indexes(entity):
            indexes.append(self._generate_index(i.name))
            if i.type == 'float':
                columns.append(f"`{i.name}` float")
            elif i.type == 'int':
                columns.append(f"`{i.name}` integer")
            elif i.type == 'datetime':
                columns.append(self._datetime_declaration(i.name))
            else:
                length = i.metadata['length'] if 'length' in i.metadata else 256
                columns.append(f"`{i.name}` varchar({length})")
        extra = ''
        if len(columns) > 0:
            self._generate_extra(columns, indexes)
            extra = self._generate_extra(columns, indexes)

        sql = f"""
            create table if not exists {self._fqtn(entity)} (
                id varchar(40)
                , obj longtext not null
                {extra}
                , primary key(id)
            )
        """
        return sql

    def raw(self, entity: Type[ffd.Entity], criteria: ffd.BinaryOp = None, limit: int = None):
        raise NotImplementedError('You cannot call raw() on data api interfaces')
