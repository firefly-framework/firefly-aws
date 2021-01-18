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

from typing import Optional

import firefly as ff
from firefly import domain as ffd

import firefly_aws.domain as domain


@ff.authenticator()
class CognitoAuthenticator(ff.Handler, ff.LoggerAware):
    _jwt_decoder: domain.JwtDecoder = None

    def handle(self, message: ffd.Message) -> Optional[bool]:
        if 'http_request' in message.headers and message.headers.get('secured', True):
            token = None
            for k, v in message.headers['http_request']['headers'].items():
                if k.lower() == 'authorization':
                    if not v.lower().startswith('bearer'):
                        raise ff.UnauthenticatedError()
                    token = v.split(' ')[-1]
            if token is None:
                raise ff.UnauthenticatedError()

            self.debug('Decoding token')
            claims = self._jwt_decoder.decode(token)
            self.debug('Result from decode: %s', claims)
            if claims is None:
                return False

            scopes = []
            if 'cognito:groups' in claims:
                groups = list(map(lambda g: g.lower(), claims['cognito:groups']))
                self.debug('cognito:groups: %s', groups)
                for client_claim in claims['scope'].split(' '):
                    client_claim = client_claim.replace('/', '.').lower()
                    self.debug('%s in %s', client_claim, groups)
                    if client_claim in groups:
                        scopes.append(client_claim)
            elif claims['client_id'] == claims['sub']:
                scopes = list(map(lambda c: c.replace('/', '.').lower(), claims['scope'].split(' ')))
                for scope in scopes:
                    if scope.startswith('tenant.'):
                        message.headers['tenant'] = scope.split(':')[1]
                        break

            claims['scopes'] = scopes
            self.debug('Scopes: %s', claims['scopes'])

            self.debug('Got sub: %s', claims['sub'])
            message.headers['decoded_token'] = claims
            return True

        return message.headers.get('secured') is not True
