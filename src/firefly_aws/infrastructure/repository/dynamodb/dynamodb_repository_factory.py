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

from firefly_aws.infrastructure.repository.dynamodb.dynamodb_repository import DynamodbRepository

E = TypeVar('E', bound=ff.Entity)


class DynamodbRepositoryFactory(ff.RepositoryFactory):
    _context_map: ff.ContextMap = None
    _container: di.Container = None

    def __init__(self, client):
        self._client = client

    def __call__(self, entity: Type[E]) -> ff.Repository:
        class Repo(DynamodbRepository[entity]):
            pass

        return self._container.build(Repo)
