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

from math import ceil
from typing import Type

import firefly as ff

from .data_api_storage_interface import DataApiStorageInterface


class DataApiMysqlStorageInterface(DataApiStorageInterface):
    def __init__(self):
        super().__init__()

    def _get_average_row_size(self, entity: Type[ff.Entity]):
        result = ff.retry(
            lambda: self._exec(f"select CEIL(AVG(LENGTH(obj))) from {self._fqtn(entity)}", [])
        )
        return result['records'][0][0]['longValue'] / 1024
