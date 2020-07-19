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
from firefly import Repository

from .cognito_repository import CognitoRepository

E = TypeVar('E', bound=ff.Entity)


class CognitoRepositoryFactory(ff.RepositoryFactory):
    def __init__(self, cognito_idp_client):
        self._cognito_idp_client = cognito_idp_client

    def __call__(self, entity: Type[E]) -> Repository:
        class Repo(CognitoRepository[entity]):
            pass

        return Repo(self._cognito_idp_client)
