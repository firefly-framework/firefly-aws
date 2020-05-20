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

import importlib
from typing import List

import firefly as ff
from troposphere import AWSObject, BaseAWSObject


class Stack(ff.Entity):
    id: str = ff.id_()
    resources: List[AWSObject] = ff.list_()
    parameters: List[BaseAWSObject] = ff.list_()

    def __post_init__(self):
        for t in ['resources', 'parameters']:
            converted = []
            for resource in getattr(self, t):
                if isinstance(resource, dict) and 'module' in resource:
                    module = importlib.import_module(resource['module'])
                    type_ = module.get(resource['type'])
                    converted.append(type_.from_dict(resource['data']))
            setattr(self, t, converted)

    def to_dict(self):
        ret = {
            'id': self.id,
        }
        for t in ['resources', 'parameters']:
            ret[t] = [
                {
                    'module': r.__class__.__module__,
                    'type': r.__class__.__name__,
                    'data': r.to_dict(),
                }
                for r in getattr(self, t)
            ]
        return ret

    def num_resources(self):
        return len(self.resources) + len(self.parameters)

    def has_resource(self, title: str):
        for resource in self.resources:
            if resource.title == title:
                return True
        return False

    def get_resource(self, title: str):
        for resource in self.resources:
            if resource.title == title:
                return resource
