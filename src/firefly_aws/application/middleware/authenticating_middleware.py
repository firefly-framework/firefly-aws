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

import firefly_aws.domain as domain
import firefly as ff
from firefly import domain as ffd


@ff.register_middleware(index=1)
class AuthenticatingMiddleware(ff.Middleware, ff.LoggerAware):
    _jwt_decoder: domain.JwtDecoder = None

    def __call__(self, message: ffd.Message, next_: Callable) -> ffd.Message:
        if 'http_request' in message.headers and message.headers.get('secured', True):
            token = None
            for k, v in message.headers['http_request']['headers'].items():
                if k.lower() == 'authorization':
                    if not v.startswith('Bearer'):
                        raise ff.UnauthenticatedError()
                    token = v.split(' ')[-1]
            if token is None:
                raise ff.UnauthenticatedError()

            self.debug('Decoding token')
            claims = self._jwt_decoder.decode(token)
            self.debug('Got sub: %s', claims['sub'])
            message.headers['sub'] = claims['sub']

        return next_(message)
