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

from typing import Type, TypeVar

import firefly as ff
import firefly_di as di
from firefly_aws.infrastructure.repository.s3.s3_repository import S3Repository

E = TypeVar('E', bound=ff.Entity)


class S3RepositoryFactory(ff.RepositoryFactory):
    _context_map: ff.ContextMap = None
    _container: di.Container = None

    def __init__(self, client, prefix: str = 'aggregates'):
        self._client = client
        self._prefix = prefix

    def __call__(self, entity: Type[E]) -> ff.Repository:
        class Repo(S3Repository[entity]):
            pass

        config = self._context_map.get_context('firefly_aws').config

        return Repo(
            self._container.s3_client, self._container.serializer, bucket=config.get('bucket'), prefix=self._prefix
        )
