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

import json
import re
from dataclasses import fields
from pprint import pprint
from typing import Type, Union, Callable, Tuple, List

import firefly as ff
import firefly.infrastructure as ffi
from botocore.exceptions import ClientError
from dynamodb_json import json_util
from firefly import domain as ffd

from .deconstruct_entity import DeconstructEntity
from .generate_key_and_filter_expressions import GenerateKeyAndFilterExpressions
from .generate_pk import GeneratePk
from .reconstruct_entity import ReconstructEntity


# noinspection PyDataclass
class DynamodbStorageInterface(ffi.AbstractStorageInterface):
    _serializer: ffi.JsonSerializer = None
    _batch_process: ffd.BatchProcess = None
    _generate_pk: GeneratePk = None
    _generate_key_and_filter_expressions: GenerateKeyAndFilterExpressions = None
    _deconstruct_entity: DeconstructEntity = None
    _reconstruct_entity: ReconstructEntity = None
    _ddb_client = None
    _ddb_table: str = None
    _cache: dict = None
    _table: str = None

    def __init__(self, table: str = None, **kwargs):
        super().__init__(**kwargs)
        self._table = table

    def _add(self, entity: List[ffd.Entity]):
        return self._store(self._deconstruct_entities(entity))

    def _deconstruct_entities(self, entity: List[ffd.Entity]):
        return [self._deconstruct_entity(e) for e in entity]

    def _get_indexes(self, type_: Type[ffd.Entity]) -> dict:
        ret = {}
        for field_ in fields(type_):
            idx = field_.metadata.get('index')
            if idx is not None:
                if not isinstance(idx, int):
                    raise ff.ConfigurationError('Dynamodb indexes must be integers')
                if idx < 1:
                    raise ff.ConfigurationError(f"Got index {idx}. Dynamodb indexes start at 1")
                if idx not in ret:
                    ret[idx] = []
                mismatches = list(filter(lambda i: i['field'] != field_.name, ret[idx]))
                if len(mismatches) > 0:
                    raise ff.ConfigurationError(
                        f"Can't construct index (gsi_{idx}) for field {field_.name}. There are other fields with the "
                        f"same index: {', '.join(list(map(lambda i: i['field'], mismatches)))}"
                    )
                ret[idx].append({
                    'field': field_.name,
                    'index': idx,
                    'order': field_.metadata.get('order', 1),
                })

        return ret

    def _store(self, aggregates: List[dict]):
        for entities in aggregates:
            root = list(filter(lambda e: e['sk'] == 'root', entities)).pop()
            if 'ff_version' not in root:
                v = 1
                root['ff_version'] = 1
            else:
                v = root['ff_version']
                root['ff_version'] += 1
            self._do_store(root, check_version=v)
            parts = list(filter(lambda e: e['sk'] != 'root', entities))
            for part in parts:
                self._do_store(part)

        return len(aggregates)

    def _do_store(self, data: dict, check_version: Union[bool, int] = False):
        args = {
            'TableName': self._ddb_table,
            'Item': json.loads(json_util.dumps(data)),
        }

        if check_version:
            args['ConditionExpression'] = "attribute_not_exists(pk) or ff_version = :version"
            print(data)
            args['ExpressionAttributeValues'] = json.loads(json_util.dumps({
                ':version': check_version,
            }))

        try:
            self._ddb_client.put_item(**args)
        except ClientError as e:
            if 'conditional request failed' in str(e):
                raise ff.ConcurrentUpdateDetected()
            raise e

    def _all(self, entity_type: Type[ffd.Entity], criteria: ffd.BinaryOp = None, limit: int = None, offset: int = None,
             sort: Tuple[Union[str, Tuple[str, bool]]] = None, raw: bool = False, count: bool = False):
        params = {
            'TableName': self._ddb_table,
            'Select': 'ALL_ATTRIBUTES',
        }

        if count is not False:
            params['Select'] = 'COUNT'

        if criteria is not None:
            key_expression, key_bindings, index, filter_expression, filter_bindings = \
                self._generate_key_and_filter_expressions(criteria, self._get_indexes(entity_type))
            combined = key_bindings.copy()
            if 'pk = ' not in key_expression:
                for k, v in filter_bindings.items():
                    if k not in combined:
                        combined[k] = v
            bindings = {}
            for k, v in combined.items():
                bindings[f":{k}"] = v

            if 'pk = ' not in key_expression:
                key_expression = f"begins_with(pk, :entity_type) AND {key_expression}"
                bindings[':entity_type'] = entity_type.__name__
                params.update({
                    'FilterExpression': filter_expression,
                })

            if index is not None:
                params['IndexName'] = index

            params.update({
                'KeyConditionExpression': key_expression,
                'ExpressionAttributeValues': json_util.dumps(bindings, as_dict=True),
            })
        elif count is False:
            params.update({
                'KeyConditionExpression': f"pk = :entity_type",
                'ExpressionAttributeValues': json_util.dumps({
                    ':entity_type': f'{entity_type.__name__}#',
                }, as_dict=True),
            })
        else:
            params.update({
                'IndexName': 'gsi_20',
                'KeyConditionExpression': "gsi_20 = :entity_type and sk = :root",
                'ExpressionAttributeValues': json_util.dumps({
                    ':entity_type': entity_type.__name__,
                    ':root': 'root',
                }, as_dict=True),
            })

        items = self._query(params)
        if count is not False and isinstance(items, int):
            return items

        if len(items) == 0:
            return []

        items = self._reconstruct_entity(entity_type, items)

        if limit is not None:
            offset = offset or 0
            end = offset + limit
            items = items[offset:end]

        if count is not False:
            print(items)
            return len(items)

        if raw is True:
            return list(map(lambda e: e.to_dict(), items))

        return items

    def _find(self, uuid: Union[str, Callable], entity_type: Type[ffd.Entity]):
        if isinstance(uuid, str):
            def _criteria(e):
                return e.pk == self._generate_pk(entity_type, id_=uuid)
            criteria = _criteria(ff.EntityAttributeSpy())
        else:
            criteria = uuid(ff.EntityAttributeSpy())

        items = self._all(entity_type, criteria)
        if len(items) == 0:
            return None

        if len(items) > 1:
            raise ff.MultipleResultsFound()

        return items[0]

    def _query(self, params: dict):
        params = self._fix_reserved_words(params)
        pprint(params)
        items = []
        while True:
            response = self._ddb_client.query(**params)
            if 'Count' in response and 'Items' not in response:
                items = int(response['Count'])
                break

            items.extend(response['Items'])
            if 'LastEvaluatedKey' not in response:
                break
            params['ExclusiveStartKey'] = response['LastEvaluatedKey']

        return items

    def _fix_reserved_words(self, params: dict):
        if 'KeyConditionExpression' not in params:
            return params

        counter = 1
        attribute_names = {}
        condition = params['KeyConditionExpression'].lower()
        for word in RESERVED_WORDS:
            if f"{word.lower()} " in condition:
                attr = f"#attr_{counter}"
                counter += 1
                condition = condition.replace(f"{word.lower()} ", f"{attr} ")
                attribute_names[attr] = word.lower()

        params['KeyConditionExpression'] = condition
        if len(attribute_names.keys()) > 0:
            params['ExpressionAttributeNames'] = attribute_names

        return params

    def _remove(self, entity: ffd.Entity):
        if not isinstance(entity, list):
            entity = [entity]

        for e in entity:
            for part in self._deconstruct_entity(e):
                self._ddb_client.delete_item(
                    TableName=self._ddb_table,
                    Key=json_util.dumps({
                        'pk': part['pk'],
                        'sk': part['sk'],
                    }, as_dict=True)
                )

    def _update(self, entity: ffd.Entity):
        if not isinstance(entity, list):
            entity = [entity]
        self._add(entity)

    def _disconnect(self):
        pass

    def _ensure_connected(self):
        return True

    def clear(self, entity: Type[ffd.Entity]):
        pass

    def destroy(self, entity: Type[ffd.Entity]):
        pass

    def _build_entity(self, entity: Type[ffd.Entity], data, raw: bool = False):
        pass


RESERVED_WORDS = (
    'ABORT', 'ABSOLUTE', 'ACTION', 'ADD', 'AFTER', 'AGENT', 'AGGREGATE', 'ALL', 'ALLOCATE', 'ALTER', 'ANALYZE',
    'ANY', 'ARCHIVE', 'ARE', 'ARRAY', 'AS', 'ASC', 'ASCII', 'ASENSITIVE', 'ASSERTION', 'ASYMMETRIC', 'AT', 'ATOMIC',
    'ATTACH', 'ATTRIBUTE', 'AUTH', 'AUTHORIZATION', 'AUTHORIZE', 'AUTO', 'AVG', 'BACK', 'BACKUP', 'BASE', 'BATCH',
    'BEFORE', 'BEGIN', 'BETWEEN', 'BIGINT', 'BINARY', 'BIT', 'BLOB', 'BLOCK', 'BOOLEAN', 'BOTH', 'BREADTH', 'BUCKET',
    'BULK', 'BY', 'BYTE', 'CALL', 'CALLED', 'CALLING', 'CAPACITY', 'CASCADE', 'CASCADED', 'CASE', 'CAST', 'CATALOG',
    'CHAR', 'CHARACTER', 'CHECK', 'CLASS', 'CLOB', 'CLOSE', 'CLUSTER', 'CLUSTERED', 'CLUSTERING', 'CLUSTERS',
    'COALESCE', 'COLLATE', 'COLLATION', 'COLLECTION', 'COLUMN', 'COLUMNS', 'COMBINE', 'COMMENT', 'COMMIT', 'COMPACT',
    'COMPILE', 'COMPRESS', 'CONDITION', 'CONFLICT', 'CONNECT', 'CONNECTION', 'CONSISTENCY', 'CONSISTENT', 'CONSTRAINT',
    'CONSTRAINTS', 'CONSTRUCTOR', 'CONSUMED', 'CONTINUE', 'CONVERT', 'COPY', 'CORRESPONDING', 'COUNT', 'COUNTER',
    'CREATE', 'CROSS', 'CUBE', 'CURRENT', 'CURSOR', 'CYCLE', 'DATA', 'DATABASE', 'DATE', 'DATETIME', 'DAY',
    'DEALLOCATE', 'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DEFERRABLE', 'DEFERRED', 'DEFINE', 'DEFINED', 'DEFINITION',
    'DELETE', 'DELIMITED', 'DEPTH', 'DEREF', 'DESC', 'DESCRIBE', 'DESCRIPTOR', 'DETACH', 'DETERMINISTIC', 'DIAGNOSTICS',
    'DIRECTORIES', 'DISABLE', 'DISCONNECT', 'DISTINCT', 'DISTRIBUTE', 'DO', 'DOMAIN', 'DOUBLE', 'DROP', 'DUMP',
    'DURATION', 'DYNAMIC', 'EACH', 'ELEMENT', 'ELSE', 'ELSEIF', 'EMPTY', 'ENABLE', 'END', 'EQUAL', 'EQUALS', 'ERROR',
    'ESCAPE', 'ESCAPED', 'EVAL', 'EVALUATE', 'EXCEEDED', 'EXCEPT', 'EXCEPTION', 'EXCEPTIONS', 'EXCLUSIVE', 'EXEC',
    'EXECUTE', 'EXISTS', 'EXIT', 'EXPLAIN', 'EXPLODE', 'EXPORT', 'EXPRESSION', 'EXTENDED', 'EXTERNAL', 'EXTRACT',
    'FAIL', 'FALSE', 'FAMILY', 'FETCH', 'FIELDS', 'FILE', 'FILTER', 'FILTERING', 'FINAL', 'FINISH', 'FIRST', 'FIXED',
    'FLATTERN', 'FLOAT', 'FOR', 'FORCE', 'FOREIGN', 'FORMAT', 'FORWARD', 'FOUND', 'FREE', 'FROM', 'FULL', 'FUNCTION',
    'FUNCTIONS', 'GENERAL', 'GENERATE', 'GET', 'GLOB', 'GLOBAL', 'GO', 'GOTO', 'GRANT', 'GREATER', 'GROUP', 'GROUPING',
    'HANDLER', 'HASH', 'HAVE', 'HAVING', 'HEAP', 'HIDDEN', 'HOLD', 'HOUR', 'IDENTIFIED', 'IDENTITY', 'IF', 'IGNORE',
    'IMMEDIATE', 'IMPORT', 'IN', 'INCLUDING', 'INCLUSIVE', 'INCREMENT', 'INCREMENTAL', 'INDEX', 'INDEXED', 'INDEXES',
    'INDICATOR', 'INFINITE', 'INITIALLY', 'INLINE', 'INNER', 'INNTER', 'INOUT', 'INPUT', 'INSENSITIVE', 'INSERT',
    'INSTEAD', 'INT', 'INTEGER', 'INTERSECT', 'INTERVAL', 'INTO', 'INVALIDATE', 'IS', 'ISOLATION', 'ITEM', 'ITEMS',
    'ITERATE', 'JOIN', 'KEY', 'KEYS', 'LAG', 'LANGUAGE', 'LARGE', 'LAST', 'LATERAL', 'LEAD', 'LEADING', 'LEAVE', 'LEFT',
    'LENGTH', 'LESS', 'LEVEL', 'LIKE', 'LIMIT', 'LIMITED', 'LINES', 'LIST', 'LOAD', 'LOCAL', 'LOCALTIME',
    'LOCALTIMESTAMP', 'LOCATION', 'LOCATOR', 'LOCK', 'LOCKS', 'LOG', 'LOGED', 'LONG', 'LOOP', 'LOWER', 'MAP', 'MATCH',
    'MATERIALIZED', 'MAX', 'MAXLEN', 'MEMBER', 'MERGE', 'METHOD', 'METRICS', 'MIN', 'MINUS', 'MINUTE', 'MISSING', 'MOD',
    'MODE', 'MODIFIES', 'MODIFY', 'MODULE', 'MONTH', 'MULTI', 'MULTISET', 'NAME', 'NAMES', 'NATIONAL', 'NATURAL',
    'NCHAR', 'NCLOB', 'NEW', 'NEXT', 'NO', 'NONE', 'NOT', 'NULL', 'NULLIF', 'NUMBER', 'NUMERIC', 'OBJECT', 'OF',
    'OFFLINE', 'OFFSET', 'OLD', 'ON', 'ONLINE', 'ONLY', 'OPAQUE', 'OPEN', 'OPERATOR', 'OPTION', 'ORDER',
    'ORDINALITY', 'OTHER', 'OTHERS', 'OUT', 'OUTER', 'OUTPUT', 'OVER', 'OVERLAPS', 'OVERRIDE', 'OWNER', 'PAD',
    'PARALLEL', 'PARAMETER', 'PARAMETERS', 'PARTIAL', 'PARTITION', 'PARTITIONED', 'PARTITIONS', 'PATH', 'PERCENT',
    'PERCENTILE', 'PERMISSION', 'PERMISSIONS', 'PIPE', 'PIPELINED', 'PLAN', 'POOL', 'POSITION', 'PRECISION', 'PREPARE',
    'PRESERVE', 'PRIMARY', 'PRIOR', 'PRIVATE', 'PRIVILEGES', 'PROCEDURE', 'PROCESSED', 'PROJECT', 'PROJECTION',
    'PROPERTY', 'PROVISIONING', 'PUBLIC', 'PUT', 'QUERY', 'QUIT', 'QUORUM', 'RAISE', 'RANDOM', 'RANGE', 'RANK', 'RAW',
    'READ', 'READS', 'REAL', 'REBUILD', 'RECORD', 'RECURSIVE', 'REDUCE', 'REF', 'REFERENCE', 'REFERENCES',
    'REFERENCING', 'REGEXP', 'REGION', 'REINDEX', 'RELATIVE', 'RELEASE', 'REMAINDER', 'RENAME', 'REPEAT', 'REPLACE',
    'REQUEST', 'RESET', 'RESIGNAL', 'RESOURCE', 'RESPONSE', 'RESTORE', 'RESTRICT', 'RESULT', 'RETURN', 'RETURNING',
    'RETURNS', 'REVERSE', 'REVOKE', 'RIGHT', 'ROLE', 'ROLES', 'ROLLBACK', 'ROLLUP', 'ROUTINE', 'ROW', 'ROWS', 'RULE',
    'RULES', 'SAMPLE', 'SATISFIES', 'SAVE', 'SAVEPOINT', 'SCAN', 'SCHEMA', 'SCOPE', 'SCROLL', 'SEARCH', 'SECOND',
    'SECTION', 'SEGMENT', 'SEGMENTS', 'SELECT', 'SELF', 'SEMI', 'SENSITIVE', 'SEPARATE', 'SEQUENCE', 'SERIALIZABLE',
    'SESSION', 'SET', 'SETS', 'SHARD', 'SHARE', 'SHARED', 'SHORT', 'SHOW', 'SIGNAL', 'SIMILAR', 'SIZE', 'SKEWED',
    'SMALLINT', 'SNAPSHOT', 'SOME', 'SOURCE', 'SPACE', 'SPACES', 'SPARSE', 'SPECIFIC', 'SPECIFICTYPE', 'SPLIT', 'SQL',
    'SQLCODE', 'SQLERROR', 'SQLEXCEPTION', 'SQLSTATE', 'SQLWARNING', 'START', 'STATE', 'STATIC', 'STATUS', 'STORAGE',
    'STORE', 'STORED', 'STREAM', 'STRING', 'STRUCT', 'STYLE', 'SUB', 'SUBMULTISET', 'SUBPARTITION', 'SUBSTRING',
    'SUBTYPE', 'SUM', 'SUPER', 'SYMMETRIC', 'SYNONYM', 'SYSTEM', 'TABLE', 'TABLESAMPLE', 'TEMP', 'TEMPORARY',
    'TERMINATED', 'TEXT', 'THAN', 'THEN', 'THROUGHPUT', 'TIME', 'TIMESTAMP', 'TIMEZONE', 'TINYINT', 'TO', 'TOKEN',
    'TOTAL', 'TOUCH', 'TRAILING', 'TRANSACTION', 'TRANSFORM', 'TRANSLATE', 'TRANSLATION', 'TREAT', 'TRIGGER', 'TRIM',
    'TRUE', 'TRUNCATE', 'TTL', 'TUPLE', 'TYPE', 'UNDER', 'UNDO', 'UNION', 'UNIQUE', 'UNIT', 'UNKNOWN', 'UNLOGGED',
    'UNNEST', 'UNPROCESSED', 'UNSIGNED', 'UNTIL', 'UPDATE', 'UPPER', 'URL', 'USAGE', 'USE', 'USER', 'USERS', 'USING',
    'UUID', 'VACUUM', 'VALUE', 'VALUED', 'VALUES', 'VARCHAR', 'VARIABLE', 'VARIANCE', 'VARINT', 'VARYING', 'VIEW',
    'VIEWS', 'VIRTUAL', 'VOID', 'WAIT', 'WHEN', 'WHENEVER', 'WHERE', 'WHILE', 'WINDOW', 'WITH', 'WITHIN', 'WITHOUT',
    'WORK', 'WRAPPED', 'WRITE', 'YEAR', 'ZONE'
)