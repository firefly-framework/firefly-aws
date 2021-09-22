from __future__ import annotations

import json
import re
from pprint import pprint
from typing import Type

import firefly as ff
from dynamodb_json import json_util


class ReconstructEntity(ff.Dependency):
    def __init__(self):
        self._array = re.compile(r'([^\[]+)\[(\d+)')
        self._dict = re.compile(r'([^{]+){([^}]+)')

    def __call__(self, type_: Type[ff.Entity], data: list):
        data = list(map(lambda r: json_util.loads(json.dumps(r)), data))
        records = self._split_records(data)
        return list(map(lambda r: self._reconstruct_entity(type_, r), records.values()))

    def _reconstruct_entity(self, type_: Type[ff.Entity], data: list):
        root = list(filter(lambda x: x['sk'] == 'root', data)).pop()
        parts = list(filter(lambda x: x['sk'] != 'root', data))

        i = 0
        while len(parts) > 0:
            record = parts.pop(0)
            path = record['sk'].split('.')
            r = root
            for prop_index, prop in enumerate(path):
                last = prop_index >= (len(path) - 1)
                if prop.endswith(']'):
                    m = self._array.match(prop)
                    k = m[1]
                    i = int(m[2])
                    if k not in r:
                        r[k] = []
                    if last:
                        r[k].insert(i, record)
                    r = r[k]

                elif prop.endswith('}'):
                    m = self._dict.match(prop)
                    k = m[1]
                    i = m[2]
                    if k not in r:
                        r[k] = {}
                    if last:
                        r[k][i] = record
                    r = r[k][i]

                else:
                    if isinstance(r, list):
                        try:
                            r = r[i]
                        except IndexError:
                            r.insert(i, {})
                            r = r[i]
                    r[prop] = {}
                    if last:
                        r[prop] = record
                    r = r[prop]

        return type_.from_dict(root)

    def _split_records(self, data: list) -> dict:
        ret = {}
        for record in data:
            if record['pk'] not in ret:
                ret[record['pk']] = []
            ret[record['pk']].append(record)

        return ret
