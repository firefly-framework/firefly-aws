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

from typing import Callable

import cognitojwt
import firefly as ff
from firefly import domain as ffd


@ff.register_middleware(index=1)
class AuthenticatingMiddleware(ff.Middleware, ff.LoggerAware):
    _region: str = None
    _cognito_id: str = None
    _app_client_id: str = None

    def __call__(self, message: ffd.Message, next_: Callable) -> ffd.Message:
        self.info(f"secured: {message.headers.get('secured', True)}")
        if 'http_request' in message.headers and message.headers.get('secured', True):
            token = None
            for k, v in message.headers['http_request']['headers'].items():
                if k.lower() == 'authorization':
                    token = v
            if token is None:
                raise ff.UnauthenticatedError()

            claims = cognitojwt.decode(
                token,
                self._region,
                self._cognito_id
            )
            message.headers['sub'] = claims['sub']

        return next_(message)
