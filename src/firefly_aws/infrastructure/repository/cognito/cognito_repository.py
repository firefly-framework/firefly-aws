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

from typing import Union, Callable, Optional, List

import firefly as ff
from firefly import domain as ffd
from firefly.domain.repository.repository import T


class CognitoRepository(ff.Repository):
    def __init__(self, cognito_idp_client, **kwargs):
        super().__init__()
        self._client = cognito_idp_client

    def append(self, entity: T, **kwargs):
        pass

    def remove(self, entity: T, **kwargs):
        pass

    def find(self, exp: Union[str, Callable], **kwargs) -> Optional[T]:
        pass

    def filter(self, cb: Callable, **kwargs) -> List[T]:
        pass

    def reduce(self, cb: Callable) -> Optional[T]:
        pass

    def __iter__(self):
        pass

    def __next__(self):
        pass

    def __len__(self):
        pass

    def __getitem__(self, item):
        pass

    def commit(self, **kwargs):
        pass

    def execute_ddl(self):
        pass

    def raw(self, cb: Union[Callable, ffd.BinaryOp] = None, limit: int = None):
        pass
