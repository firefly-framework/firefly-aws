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

import os
import shutil
from datetime import datetime
from time import sleep

import firefly as ff
import inflection
import yaml
from botocore.exceptions import ClientError
from firefly_aws import S3Service, ResourceNameAware
from troposphere import Template, GetAtt, Ref, Parameter, Output, Export, ImportValue, Join
from troposphere.apigatewayv2 import Api, Stage, Deployment, Integration, Route
from troposphere.awslambda import Function, Code, VPCConfig, Environment, Permission
from troposphere.constants import NUMBER
from troposphere.iam import Role, Policy
from troposphere.s3 import Bucket, LifecycleRule, LifecycleConfiguration
from troposphere.sns import Topic, SubscriptionResource
from troposphere.sqs import Queue


@ff.agent('aws')
class AwsAgent(ff.ApplicationService, ResourceNameAware):
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
        try:
            self._bucket = self._configuration.contexts.get('firefly_aws').get('bucket')
        except AttributeError:
            raise ff.FrameworkError('No deployment bucket configured in firefly_aws')

        self._project = self._configuration.all.get('project')
        aws_config = self._configuration.contexts.get('firefly_aws')
        self._region = aws_config.get('region')
        self._security_group_ids = aws_config.get('vpc').get('security_group_ids')
        self._subnet_ids = aws_config.get('vpc').get('subnet_ids')

        self._create_project_stack()

        for service in deployment.services:
            lambda_path = inflection.dasherize(self._lambda_resource_name(service.name))
            self._code_key = f'lambda/code/{lambda_path}/{datetime.now().isoformat()}.zip'
            self._deploy_service(service)

    def _deploy_service(self, service: ff.Service):
        context = self._context_map.get_context(service.name)
        self._package_and_deploy_code(context)

        template = Template()
        template.set_version('2010-09-09')

        memory_size = template.add_parameter(Parameter(
            f'{self._lambda_resource_name(service.name)}MemorySize',
            Type=NUMBER,
            Default='3008',

        ))

        timeout_gateway = template.add_parameter(Parameter(
            f'{self._lambda_resource_name(service.name)}GatewayTimeout',
            Type=NUMBER,
            Default='30'
        ))

        timeout_async = template.add_parameter(Parameter(
            f'{self._lambda_resource_name(service.name)}AsyncTimeout',
            Type=NUMBER,
            Default='900'
        ))

        role_title = f'{self._lambda_resource_name(service.name)}ExecutionRole'
        self._add_role(role_title, template)

        api_lambda = template.add_resource(Function(
            f'{self._lambda_resource_name(service.name)}Sync',
            FunctionName=f'{self._service_name(service.name)}Sync',
            Code=Code(
                S3Bucket=self._bucket,
                S3Key=self._code_key
            ),
            Handler='handlers.main',
            Role=GetAtt(role_title, 'Arn'),
            Runtime='python3.7',
            MemorySize=Ref(memory_size),
            Timeout=Ref(timeout_gateway),
            VpcConfig=VPCConfig(
                SecurityGroupIds=self._security_group_ids,
                SubnetIds=self._subnet_ids
            ),
            Environment=self._lambda_environment(context)
        ))

        route = inflection.dasherize(context.name)
        proxy_route = f'{route}/{{proxy+}}'
        template.add_resource(Permission(
            f'{self._lambda_resource_name(service.name)}SyncPermission',
            Action='lambda:InvokeFunction',
            FunctionName=f'{self._service_name(service.name)}Sync',
            Principal='apigateway.amazonaws.com',
            SourceArn=Join('', [
                'arn:aws:execute-api:',
                self._region,
                ':',
                self._account_id,
                ':',
                ImportValue(self._rest_api_reference()),
                '/*/*/',
                route,
                '*'
            ]),
            DependsOn=api_lambda
        ))

        template.add_resource(Function(
            f'{self._lambda_resource_name(service.name)}Async',
            FunctionName=f'{self._service_name(service.name)}Async',
            Code=Code(
                S3Bucket=self._bucket,
                S3Key=self._code_key
            ),
            Handler='handlers.main',
            Role=GetAtt(role_title, 'Arn'),
            Runtime='python3.7',
            MemorySize=Ref(memory_size),
            Timeout=Ref(timeout_async),
            VpcConfig=VPCConfig(
                SecurityGroupIds=self._security_group_ids,
                SubnetIds=self._subnet_ids
            ),
            Environment=self._lambda_environment(context)
        ))

        integration = template.add_resource(Integration(
            self._integration_name(context.name),
            ApiId=ImportValue(self._rest_api_reference()),
            PayloadFormatVersion='2.0',
            IntegrationType='AWS_PROXY',
            IntegrationUri=Join('', [
                'arn:aws:lambda:',
                self._region,
                ':',
                self._account_id,
                ':function:',
                Ref(api_lambda),
            ]),
        ))

        template.add_resource(Route(
            f'{self._route_name(context.name)}Base',
            ApiId=ImportValue(self._rest_api_reference()),
            RouteKey=f'ANY /{route}',
            # AuthorizationType='AWS_IAM',
            AuthorizationType='NONE',
            Target=Join('/', ['integrations', Ref(integration)]),
            DependsOn=integration
        ))

        template.add_resource(Route(
            f'{self._route_name(context.name)}Proxy',
            ApiId=ImportValue(self._rest_api_reference()),
            RouteKey=f'ANY /{proxy_route}',
            # AuthorizationType='AWS_IAM',
            AuthorizationType='NONE',
            Target=Join('/', ['integrations', Ref(integration)]),
            DependsOn=integration
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
                    Endpoint=GetAtt(queue, 'Arn'),
                    TopicArn=Join('', [
                        'arn:aws:sns:',
                        self._region,
                        ':',
                        self._account_id,
                        ':',
                        self._topic_name(context.name)
                    ]),
                    FilterPolicy={
                        '_type': ['event'],
                        '_name': [x['name'] for x in list_]
                    },
                    DependsOn=[queue, topic]
                ))
            elif len(list_) > 0:
                if context_name not in self._context_map.contexts:
                    self._find_or_create_topic(context_name)
                template.add_resource(SubscriptionResource(
                    self._subscription_name(context.name, context_name),
                    Protocol='sqs',
                    Endpoint=GetAtt(queue, 'Arn'),
                    TopicArn=Join('', [
                        'arn:aws:sns:',
                        self._region,
                        ':',
                        self._account_id,
                        ':',
                        self._topic_name(context_name)
                    ]),
                    FilterPolicy={
                        '_type': ['event'],
                        '_name': [x['name'] for x in list_]
                    },
                    DependsOn=queue
                ))

        self.info('Deploying stack')
        stack_name = self._stack_name(context.name)
        try:
            self._cloudformation_client.describe_stacks(StackName=stack_name)
            self._update_stack(self._stack_name(context.name), template)
        except ClientError as e:
            if f'Stack with id {stack_name} does not exist' in str(e):
                self._create_stack(self._stack_name(context.name), template)
            else:
                raise e

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
            self._create_stack(self._stack_name(context_name), template)

    @staticmethod
    def _get_subscriptions(context: ff.Context):
        ret = []
        for service, event_types in context.event_listeners.items():
            for event_type in event_types:
                if isinstance(event_type, str):
                    context_name, event_name = event_type.split('.')
                else:
                    context_name = event_type.get_class_context()
                    event_name = event_type.__name__
                ret.append({
                    'name': event_name,
                    'context': context_name,
                })

        return ret

    def _package_and_deploy_code(self, context: ff.Context):
        self.info('Setting up build directory')
        if not os.path.isdir('./build'):
            os.mkdir('./build')
        if os.path.isdir('./build/python-sources'):
            shutil.rmtree('./build/python-sources', ignore_errors=True)
        os.mkdir('./build/python-sources')

        self.info('Installing source files')
        # TODO use setup.py instead?
        import subprocess
        subprocess.call(['pip', 'install', '-r', 'requirements.txt', '-t', './build/python-sources'])

        self.info('Packaging artifact')
        subprocess.call(['cp', 'templates/aws/handlers.py', 'build/python-sources/.'])
        os.chdir('./build/python-sources')
        with open('firefly.yml', 'w') as fp:
            fp.write(yaml.dump(self._configuration.all))

        subprocess.call(['find', '.', '-name', '*.so', '|', 'xargs', 'strip'])
        subprocess.call(['find', '.', '-name', '*.so.*', '|', 'xargs', 'strip'])
        subprocess.call(['find', '.', '-name', '*.pyc', '-delete'])
        file_name = self._code_key.split('/')[-1]
        subprocess.call(['zip', '-r', f'../{file_name}', '.'])
        os.chdir('..')

        self.info('Uploading artifact')
        with open(file_name, 'rb') as fp:
            self._s3_client.put_object(
                Body=fp.read(),
                Bucket=self._bucket,
                Key=self._code_key
            )
        os.chdir('..')

    def _create_project_stack(self):
        update = True
        try:
            self._cloudformation_client.describe_stacks(StackName=self._stack_name())
        except ClientError as e:
            if 'does not exist' not in str(e):
                raise e
            update = False

        self.info('Creating project stack')
        template = Template()
        template.set_version('2010-09-09')

        memory_size = template.add_parameter(Parameter(
            f'{self._stack_name()}MemorySize',
            Type=NUMBER,
            Default='3008',

        ))

        timeout_gateway = template.add_parameter(Parameter(
            f'{self._stack_name()}GatewayTimeout',
            Type=NUMBER,
            Default='30'
        ))

        template.add_resource(Bucket(
            inflection.camelize(inflection.underscore(self._bucket)),
            BucketName=self._bucket,
            AccessControl='Private',
            LifecycleConfiguration=LifecycleConfiguration(Rules=[
                LifecycleRule(Prefix='tmp', Status='Enabled', ExpirationInDays=1)
            ])
        ))

        api = template.add_resource(Api(
            self._rest_api_name(),
            Name=f'{inflection.humanize(self._project)} {inflection.humanize(self._env)} API',
            ProtocolType='HTTP'
        ))

        role_title = f'{self._rest_api_name()}Role'
        self._add_role(role_title, template)

        default_lambda = template.add_resource(Function(
            f'{self._rest_api_name()}Function',
            FunctionName=self._rest_api_name(),
            Code=Code(
                ZipFile='\n'.join([
                    'def handler(event, context):',
                    '    return event'
                ])
            ),
            Handler='index.handler',
            Role=GetAtt(role_title, 'Arn'),
            Runtime='python3.7',
            MemorySize=Ref(memory_size),
            Timeout=Ref(timeout_gateway)
        ))

        integration = template.add_resource(Integration(
            self._integration_name(),
            ApiId=Ref(api),
            IntegrationType='AWS_PROXY',
            PayloadFormatVersion='2.0',
            IntegrationUri=Join('', [
                'arn:aws:lambda:',
                self._region,
                ':',
                self._account_id,
                ':function:',
                Ref(default_lambda),
            ]),
            DependsOn=f'{self._rest_api_name()}Function'
        ))

        template.add_resource(Route(
            self._route_name(),
            ApiId=Ref(api),
            RouteKey='$default',
            AuthorizationType='NONE',
            Target=Join('/', ['integrations', Ref(integration)]),
            DependsOn=integration
        ))

        template.add_resource(Stage(
            f'{self._rest_api_name()}Stage',
            StageName='v1',
            ApiId=Ref(api),
            AutoDeploy=True
        ))

        template.add_resource(Deployment(
            f'{self._rest_api_name()}Deployment',
            ApiId=Ref(api),
            StageName='v1',
            DependsOn=[
                f'{self._rest_api_name()}Stage',
                self._route_name(),
                self._integration_name(),
                self._rest_api_name(),
            ]
        ))

        template.add_output([
            Output(
                self._rest_api_reference(),
                Export=Export(self._rest_api_reference()),
                Value=Ref(api)
            ),
        ])

        if update:
            self._update_stack(self._stack_name(), template)
        else:
            self._create_stack(self._stack_name(), template)

    def _add_role(self, role_name: str, template):
        return template.add_resource(Role(
            role_name,
            Path='/',
            Policies=[
                Policy(
                    PolicyName='root',
                    PolicyDocument={
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Action': ['logs:*'],
                                'Resource': 'arn:aws:logs:*:*:*',
                                'Effect': 'Allow',
                            },
                            {
                                'Action': [
                                    'ec2:*NetworkInterface',
                                    'ec2:DescribeNetworkInterfaces',
                                    's3:*',
                                ],
                                'Resource': '*',
                                'Effect': 'Allow',
                            }
                        ]
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

    def _create_stack(self, stack_name: str, template: Template):
        self._cloudformation_client.create_stack(
            StackName=stack_name,
            TemplateBody=template.to_json(),
            Capabilities=['CAPABILITY_IAM']
        )
        self._wait_for_stack(stack_name)

    def _update_stack(self, stack_name: str, template: Template):
        try:
            self._cloudformation_client.update_stack(
                StackName=stack_name,
                TemplateBody=template.to_json(),
                Capabilities=['CAPABILITY_IAM']
            )
            self._wait_for_stack(stack_name)
        except ClientError as e:
            if 'No updates are to be performed' in str(e):
                return
            raise e

    def _wait_for_stack(self, stack_name: str):
        status = self._cloudformation_client.describe_stacks(StackName=stack_name)['Stacks'][0]
        while status['StackStatus'].endswith('_IN_PROGRESS'):
            self.info('Waiting...')
            sleep(5)
            status = self._cloudformation_client.describe_stacks(StackName=stack_name)['Stacks'][0]

    def _lambda_environment(self, context: ff.Context):
        return Environment(
            'LambdaEnvironment',
            Variables={
                'PROJECT': self._project,
                'ENV': self._env,
                'ACCOUNT_ID': self._account_id,
                'CONTEXT': context.name,
                'REGION': self._region,
                'BUCKET': self._bucket,
            }
        )
