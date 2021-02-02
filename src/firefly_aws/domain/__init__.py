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

from abc import ABC

import inflection

from .entity import *
from .error import *
from .service import *


class ResourceNameAware(ABC):
    _project: str = None
    _ff_environment: str = None
    _region: str = None
    _account_id: str = None

    def _service_name(self, context: str = ''):
        slug = f'{self._project}_{self._ff_environment}_{context}'.rstrip('_')
        return f'{inflection.camelize(inflection.underscore(slug))}'

    def _lambda_resource_name(self, name: str):
        return f'{self._service_name(name)}Function'

    def _queue_name(self, context: str):
        return f'{self._service_name(context)}Queue'

    def _ddb_resource_name(self, name: str):
        return f'{self._service_name(name)}DdbTable'

    def _ddb_table_name(self, context: str):
        return f'{inflection.camelize(context)}-{self._ff_environment}'

    def _topic_name(self, context: str):
        return f'{self._service_name(context)}Topic'

    def _integration_name(self, context: str = ''):
        return f'{self._service_name(context)}Integration'

    def _route_name(self, context: str = ''):
        return f'{self._service_name(context)}Route'

    def _stack_name(self, context: str = ''):
        return f'{self._service_name(context)}Stack'

    def _subscription_name(self, queue_context: str, topic_context: str = ''):
        slug = f'{self._project}_{self._ff_environment}_{queue_context}_{topic_context}'.rstrip('_')
        return f'{inflection.camelize(inflection.underscore(slug))}Subscription'

    def _alarm_subscription_name(self, context: str):
        slug = f'{self._project}_{self._ff_environment}_{context}'
        return f'{inflection.camelize(inflection.underscore(slug))}AlertsSubscription'

    def _rest_api_name(self):
        slug = f'{self._project}_{self._ff_environment}'
        return f'{inflection.camelize(inflection.underscore(slug))}Api'

    def _rest_api_reference(self):
        return f'{self._rest_api_name()}Id'

    def _topic_arn(self, context_name: str):
        return f'arn:aws:sns:{self._region}:{self._account_id}:{self._topic_name(context_name)}'

    def _alert_topic_name(self, context: str):
        return f'{self._service_name(context)}FireflyAlerts'

    def _alert_topic_arn(self, context: str):
        return f'arn:aws:sns:{self._region}:{self._account_id}:{self._alert_topic_name(context)}'
