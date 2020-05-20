# #  Copyright (c) 2020 JD Williams
# #
# #  This file is part of Firefly, a Python SOA framework built by JD Williams. Firefly is free software; you can
# #  redistribute it and/or modify it under the terms of the GNU General Public License as published by the
# #  Free Software Foundation; either version 3 of the License, or (at your option) any later version.
# #
# #  Firefly is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
# #  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
# #  Public License for more details. You should have received a copy of the GNU Lesser General Public
# #  License along with this program.  If not, see <http://www.gnu.org/licenses/>.
# #
# #  You should have received a copy of the GNU General Public License along with Firefly. If not, see
# #  <http://www.gnu.org/licenses/>.
#
# from __future__ import annotations
#
# import base64
# import os
# import pickle
# from typing import List
#
# import firefly as ff
# import inflection
# from troposphere import AWSObject, BaseAWSObject, GetAtt, Ref, Template
# from troposphere.awslambda import Function, Code
# from troposphere.iam import Role, Policy
#
#

class Service:
    pass

# class Service(ff.Entity):
#     id: str = ff.id_(is_uuid=False)
#     template: Template = ff.optional()
#
#     def __post_init__(self):
#         if self.template is None:
#             self.template = Template()
#         else:
#             self.template = pickle.loads(base64.b64decode(self.template))
#
#     def to_dict(self):
#         return {
#             'id': self.id,
#             'template': base64.b16encode(pickle.dumps(self.template)),
#         }
#
#     def get_lambda(self):
#         return self.get_resource(self._lambda_resource_name())
#
#     def create_lambda(self, config: ff.Configuration):
#         role_title = f'{self._lambda_resource_name()}ExecutionRole'
#         self.add_resource(Role(
#             role_title,
#             Path='/',
#             Policies=[
#                 Policy(
#                     PolicyName='root',
#                     PolicyDocument={
#                         'Version': '2012-10-17',
#                         'Statement': [{
#                             'Action': ['logs:*'],
#                             'Resource': 'arn:aws:logs:*:*:*',
#                             'Effect': 'Allow',
#                         }]
#                     }
#                 )
#             ],
#             AssumeRolePolicyDocument={
#                 'Version': '2012-10-17',
#                 'Statement': [{
#                     'Action': ['sts:AssumeRole'],
#                     'Effect': 'Allow',
#                     'Principal': {
#                         'Service': ['lambda.amazonaws.com']
#                     }
#                 }]
#             }
#         ))
#
#         self.add_resource(Function(
#             self._lambda_resource_name(),
#             Code=Code(
#                 S3Bucket='',
#                 S3Key=f'{os.environ.get("ENV")}/lambda/code/{self._lambda_resource_name()}.zip'
#             ),
#             Handler='handlers.main',
#             Role=GetAtt(role_title, 'Arn'),
#             Runtime='Python3.7',
#             MemorySize=Ref
#         ))
#
#     def add_resource(self, config: AWSObject):
#         self.get_next_stack().resources.append(config)
#
#     def add_parameter(self, param: BaseAWSObject):
#         self.get_next_stack().parameters.append(param)
#
#     def get_next_stack(self):
#         i = 0
#         stack = self.stacks[i]
#         while stack.num_resources() >= 200:
#             i += 1
#             stack = self.stacks[i]
#         return stack
#
#     def get_resource(self, title: str):
#         for stack in self.stacks:
#             if stack.has_resource(title):
#                 return stack.get_resource(title)
#
#     def _lambda_resource_name(self):
#         return f'{inflection.camelize(self.id)}Function'
