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

import cognitojwt
import firefly as ff
import firefly_aws.domain as domain
from cognitojwt import CognitoJWTException


class CognitoJwtDecoder(domain.JwtDecoder, ff.LoggerAware):
    _region: str = None
    _user_pool_id: str = None

    def decode(self, token: str, client_id: str = None):
        try:
            return cognitojwt.decode(
                token,
                self._region,
                self._user_pool_id,
                app_client_id=client_id
            )
        except (KeyError, ValueError) as e:
            self.exception(e)
            return
        except CognitoJWTException as e:
            self.exception(e)
            raise ff.UnauthenticatedError()
