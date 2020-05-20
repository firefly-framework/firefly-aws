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

from datetime import datetime
from pprint import pprint
from time import sleep

import firefly as ff
import inflection
from botocore.exceptions import ClientError
from firefly_aws import S3Service
from troposphere import Template, GetAtt, Ref, Parameter, Join, ImportValue
from troposphere.apigateway import Resource, Method, Integration, IntegrationResponse, MethodResponse, Deployment
from troposphere.awslambda import Function, Code
from troposphere.constants import NUMBER
from troposphere.iam import Role, Policy
from troposphere.sns import Topic, Subscription, SubscriptionResource
from troposphere.sqs import Queue


@ff.agent('aws')
class AwsAgent(ff.ApplicationService):
    _configuration: ff.Configuration = None
    _context_map: ff.ContextMap = None
    _registry: ff.Registry = None
    _s3_client = None
    _s3_service: S3Service = None
    _sns_client = None
    _cloudformation_client = None

    def __init__(self, env: str, account_id: str):
        self._env = env
        self._account_id = account_id

    def __call__(self, deployment: ff.Deployment, **kwargs):
        self._bucket = self._configuration.contexts.get('firefly_aws').get('bucket')
        try:
            self._s3_service.ensure_bucket_exists(self._bucket)
        except AttributeError:
            raise ff.FrameworkError('No deployment bucket configured in firefly_aws')

        self._project = self._configuration.all.get('project')
        aws = self._configuration.contexts.get('firefly_aws')
        self._region = aws.get('region')
        try:
            self._api_gateway_resource = aws.get('api_gateways').get('default').get('rest_api_id')
            self._api_gateway_root_resource = aws.get('api_gateways').get('default').get('root_resource_id')
        except AttributeError:
            self._api_gateway_resource = None

        for service in deployment.services:
            lambda_path = inflection.dasherize(self._lambda_resource_name(service))
            self._code_key = f'{self._env}/lambda/code/{lambda_path}/{datetime.now().isoformat()}.zip'
            self._deploy_service(service)

    def _deploy_service(self, service: ff.Service):
        context = self._context_map.get_context(service.name)
        self._package_and_deploy_code(context)

        template = Template()
        template.set_version('2010-09-09')

        memory_size = template.add_parameter(Parameter(
            f'{self._lambda_resource_name(service)}MemorySize',
            Type=NUMBER,
            Default='3008',

        ))

        timeout_gateway = template.add_parameter(Parameter(
            f'{self._lambda_resource_name(service)}GatewayTimeout',
            Type=NUMBER,
            Default='30'
        ))

        timeout_async = template.add_parameter(Parameter(
            f'{self._lambda_resource_name(service)}AsyncTimeout',
            Type=NUMBER,
            Default='900'
        ))

        role_title = f'{self._lambda_resource_name(service)}ExecutionRole'
        template.add_resource(Role(
            role_title,
            Path='/',
            Policies=[
                Policy(
                    PolicyName='root',
                    PolicyDocument={
                        'Version': '2012-10-17',
                        'Statement': [{
                            'Action': ['logs:*'],
                            'Resource': 'arn:aws:logs:*:*:*',
                            'Effect': 'Allow',
                        }]
                    }
                )
            ],
            AssumeRolePolicyDocument={
                'Version': '2012-10-17',
                'Statement': [{
                    'Action': ['sts:AssumeRole'],
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': ['lambda.amazonaws.com']
                    }
                }]
            }
        ))

        template.add_resource(Function(
            f'{self._lambda_resource_name(service)}Gateway',
            Code=Code(
                S3Bucket=self._bucket,
                S3Key=self._code_key
            ),
            Handler='handlers.main',
            Role=GetAtt(role_title, 'Arn'),
            Runtime='python3.7',
            MemorySize=Ref(memory_size),
            Timeout=Ref(timeout_gateway)
        ))

        template.add_resource(Function(
            f'{self._lambda_resource_name(service)}Async',
            Code=Code(
                S3Bucket=self._bucket,
                S3Key=self._code_key
            ),
            Handler='handlers.main',
            Role=GetAtt(role_title, 'Arn'),
            Runtime='python3.7',
            MemorySize=Ref(memory_size),
            Timeout=Ref(timeout_async)
        ))

        methods = []
        resources = {}
        for gateway in service.api_gateways:
            for endpoint in gateway.endpoints:
                slug = inflection.camelize(inflection.underscore(
                    f'{self._project}-{service.name}-{endpoint.route.replace("/", "-")}'
                ).replace('__', '_'))

                if slug not in resources:
                    resources[slug] = template.add_resource(Resource(
                        slug,
                        RestApiId=ImportValue(self._api_gateway_resource),
                        PathPart=endpoint.route.lstrip('/'),
                        ParentId=ImportValue(self._api_gateway_root_resource)
                    ))
                template.add_resource(Method(
                    f'{slug}{inflection.camelize(endpoint.method)}',
                    DependsOn=f'{self._lambda_resource_name(service)}Gateway',
                    RestApiId=ImportValue(self._api_gateway_resource),
                    AuthorizationType="AWS_IAM",
                    ResourceId=Ref(resources[slug]),
                    HttpMethod=endpoint.method.upper(),
                    Integration=Integration(
                        Credentials=GetAtt(role_title, 'Arn'),
                        Type='AWS',
                        IntegrationHttpMethod=endpoint.method.upper(),
                        IntegrationResponses=[
                            IntegrationResponse(
                                StatusCode='200'
                            )
                        ],
                        Uri=Join("", [
                            'arn:aws:apigateway:us-west-2:lambda:path/2015-03-31/functions/',
                            GetAtt(f'{self._lambda_resource_name(service)}Gateway', 'Arn'),
                            '/invocations'
                        ])
                    ),
                    MethodResponses=[
                        MethodResponse(
                            'CatResponse',
                            StatusCode='200'
                        )
                    ]
                ))
                methods.append(f'{slug}{inflection.camelize(endpoint.method)}')
        template.add_resource(Deployment(
            self._env,
            DependsOn=methods,
            RestApiId=ImportValue(self._api_gateway_resource)
        ))

        subscriptions = {}
        for subscription in self._get_subscriptions(context):
            if subscription['context'] not in subscriptions:
                subscriptions[subscription['context']] = []
            subscriptions[subscription['context']].append(subscription)

        queue = template.add_resource(Queue(
            self._queue_name(context.name),
            QueueName=self._queue_name(context.name)
        ))
        topic = template.add_resource(Topic(
            self._topic_name(context.name),
            TopicName=self._topic_name(context.name)
        ))

        for context_name, list_ in subscriptions.items():
            if context_name == context.name and len(list_) > 0:
                template.add_resource(SubscriptionResource(
                    self._subscription_name(context_name),
                    Protocol='sqs',
                    Endpoint=Ref(queue),
                    TopicArn=Ref(topic),
                    FilterPolicy={
                        '_type': 'event',
                        '_name': [x['name'] for x in list_]
                    },
                    DependsOn=[
                        self._queue_name(context.name),
                        self._topic_name(context.name),
                    ]
                ))
            elif len(list_) > 0:
                if context_name not in self._context_map.contexts:
                    self._find_or_create_topic(context_name)
                template.add_resource(SubscriptionResource(
                    self._subscription_name(context.name, context_name),
                    Protocol='sqs',
                    Endpoint=Ref(queue),
                    TopicArn=GetAtt(self._topic_name(context_name), 'Arn'),
                    FilterPolicy={
                        '_type': 'event',
                        '_name': [x['name'] for x in list_]
                    },
                    DependsOn=[
                        self._queue_name(context.name),
                    ]
                ))

        self.info('Deploying stack')
        stack_name = self._stack_name(context.name)
        try:
            self._cloudformation_client.describe_stacks(StackName=stack_name)
            self._cloudformation_client.update_stack(
                StackName=self._stack_name(context.name),
                TemplateBody=template.to_json(),
                Capabilities=['CAPABILITY_IAM']
            )
        except ClientError as e:
            if f'Stack with id {stack_name} does not exist' in str(e):
                self._cloudformation_client.create_stack(
                    StackName=self._stack_name(context.name),
                    TemplateBody=template.to_json(),
                    Capabilities=['CAPABILITY_IAM']
                )
            else:
                raise e

        status = self._cloudformation_client.describe_stacks(StackName=self._stack_name(context.name))
        while status['StackStatus'].endswith('_IN_PROGRESS'):
            self.info('Waiting...')
            sleep(5)
            status = self._cloudformation_client.describe_stacks(StackName=self._stack_name(context.name))

        self.info('Done')

    def _find_or_create_topic(self, context_name: str):
        arn = f'arn:aws:sns:{self._region}:{self._account_id}:{self._topic_name(context_name)}'
        try:
            self._sns_client.get_topic_attributes(TopicArn=arn)
        except ClientError:
            template = Template()
            template.set_version('2010-09-09')
            template.add_resource(Topic(
                self._topic_name(context_name),
                TopicName=self._topic_name(context_name)
            ))
            self.info(f'Creating stack for context "{context_name}"')
            response = self._cloudformation_client.create_stack(
                StackName=self._stack_name(context_name),
                TemplateBody=template.to_json(),
                Capabilities=['CAPABILITY_IAM']
            )
            status = self._cloudformation_client.describe_stacks(StackName=response['StackId'])
            while status['StackStatus'].endswith('_IN_PROGRESS'):
                self.info('Waiting...')
                sleep(5)
                status = self._cloudformation_client.describe_stacks(StackName=response['StackId'])

    def _get_subscriptions(self, context: ff.Context):
        ret = []
        for ctx in self._context_map.contexts:
            for service, event_types in ctx.event_listeners.items():
                for event_type in event_types:
                    if isinstance(event_type, str):
                        context_name, event_name = event_type.split('.')
                    else:
                        context_name = event_type.get_class_context()
                        event_name = event_type.__name__
                    if context_name == context.name:
                        ret.append({
                            'name': event_name,
                            'context': context_name,
                        })

        return ret

    def _package_and_deploy_code(self, context: ff.Context):
        pass

    def _lambda_resource_name(self, service: ff.Service):
        slug = f'{self._project}-{self._env}-{service.name}'
        return f'{inflection.camelize(inflection.underscore(slug))}Function'

    def _queue_name(self, context: str):
        slug = f'{self._project}_{self._env}_{context}'
        return f'{inflection.camelize(inflection.underscore(slug))}Queue'

    def _topic_name(self, context: str):
        slug = f'{self._project}_{self._env}_{context}'
        return f'{inflection.camelize(inflection.underscore(slug))}Topic'

    def _stack_name(self, context: str):
        slug = f'{self._project}_{self._env}_{context}'
        return f'{inflection.camelize(inflection.underscore(slug))}Stack'

    def _subscription_name(self, queue_context: str, topic_context: str = ''):
        slug = f'{self._project}_{self._env}_{queue_context}_{topic_context}'
        return f'{inflection.camelize(inflection.underscore(slug))}Subscription'
