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

from typing import List

import firefly as ff

from .service import Service
from .stack import Stack


class Project(ff.AggregateRoot):
    id: str = ff.id_(is_uuid=False)
    services: List[Service] = ff.list_()

    def plan_deployment(self, deployment: ff.Deployment, config: ff.Configuration):
        project = config.all.get('project')
        aws = config.contexts.get('firefly_aws')
        try:
            api_gateway_resource = aws.get('api_gateways').get('default')
        except AttributeError:
            api_gateway_resource = None

        for s in deployment.services:
            id_ = f'{project}-{s.name}'
            service = self.get_service(id_) or Service(id=id_)
            function = service.get_lambda()

            for gateway in s.api_gateways:
                for endpoint in gateway.endpoints:
                    print(endpoint.route)

            for topic in s.network_topology.topics:
                print(f'{topic.name}:')
                for sub in topic.subscribers:
                    print(f'  {sub.name}')

    def get_service(self, id_: str):
        for s in self.services:
            if s.id == id_:
                return s
