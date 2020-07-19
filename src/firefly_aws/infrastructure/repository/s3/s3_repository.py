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

from typing import List, Callable, Optional, Union

import firefly as ff
import inflection
from botocore.exceptions import ClientError
from firefly import domain as ffd
from firefly.domain.repository.repository import T


class S3Repository(ff.Repository[T]):
    def __init__(self, s3_client, serializer: ff.Serializer, bucket: str, prefix: str = 'object-store/aggregates'):
        self._s3_client = s3_client
        self._serializer = serializer
        self._bucket = bucket
        name = inflection.pluralize(inflection.dasherize(inflection.underscore(self._type().__name__)))
        self._storage_path = f'{prefix}/{name}'.lstrip('/')

    def add(self, entity: T):
        try:
            self._s3_client.put_object(
                Bucket=self._bucket,
                Key=f'{self._storage_path}/{entity.id_value()}.json',
                Body=self._serializer.serialize(entity.to_dict()),
            )
        except ClientError as e:
            raise ff.RepositoryError(str(e))

    def remove(self, entity: T):
        try:
            self._s3_client.delete_object(
                Bucket=self._bucket,
                Key=f'{self._storage_path}/{entity.id_value()}',
            )
        except ClientError as e:
            raise ff.RepositoryError(str(e))

    def find(self, exp: Union[str, Callable]) -> Optional[T]:
        if isinstance(exp, str):
            try:
                response = self._s3_client.get_object(
                    Bucket=self._bucket,
                    Key=f'{self._storage_path}/{exp}.json',
                )
                data = self._serializer.deserialize(response['Body'].read())
                return self._type().from_dict(data)
            except ClientError as e:
                if 'NoSuchKey' in str(e):
                    return None
                raise ff.RepositoryError(str(e))

        # TODO implement search criteria

    def filter(self, cb: Callable) -> List[T]:
        # TODO implement search criteria
        pass

    def reduce(self, cb: Callable) -> Optional[T]:
        # TODO implement search criteria
        pass

    def append(self, entity: T):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

    def execute_ddl(self):
        raise NotImplementedError()

    def raw(self, cb: Union[Callable, ffd.BinaryOp] = None, limit: int = None):
        raise NotImplementedError()

    def __iter__(self):
        pass

    def __next__(self):
        pass

    def __len__(self):
        pass

    def __getitem__(self, item):
        pass
