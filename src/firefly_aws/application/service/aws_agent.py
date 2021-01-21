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
import firefly.infrastructure as ffi
import inflection
import yaml
from botocore.exceptions import ClientError
from troposphere.dynamodb import Table, AttributeDefinition, KeySchema
from troposphere.events import Target, Rule

from firefly_aws import S3Service, ResourceNameAware
from troposphere import Template, GetAtt, Ref, Parameter, Output, Export, ImportValue, Join, Equals
from troposphere.apigatewayv2 import Api, Stage, Deployment, Integration, Route
from troposphere.awslambda import Function, Code, VPCConfig, Environment, Permission, EventSourceMapping
from troposphere.cloudwatch import Alarm, MetricDimension
from troposphere.constants import NUMBER
from troposphere.iam import Role, Policy
from troposphere.s3 import Bucket, LifecycleRule, LifecycleConfiguration
from troposphere.sns import Topic, SubscriptionResource
from troposphere.sqs import Queue, QueuePolicy, RedrivePolicy


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

        self._deployment = deployment
        self._project = self._configuration.all.get('project')
        aws_config = self._configuration.contexts.get('firefly_aws')
        self._aws_config = aws_config
        self._region = aws_config.get('region')
        self._security_group_ids = aws_config.get('vpc', {}).get('security_group_ids')
        self._subnet_ids = aws_config.get('vpc', {}).get('subnet_ids')

        self._create_project_stack()

        for service in deployment.services:
            lambda_path = inflection.dasherize(self._lambda_resource_name(service.name))
            self._code_path = f'lambda/code/{lambda_path}'
            self._code_key = f'{self._code_path}/{datetime.now().isoformat()}.zip'
            self._deploy_service(service)

    def _deploy_service(self, service: ff.Service):
        context = self._context_map.get_context(service.name)
        self._package_and_deploy_code(context)

        template = Template()
        template.set_version('2010-09-09')

        memory_size = template.add_parameter(Parameter(
            f'{self._lambda_resource_name(service.name)}MemorySize',
            Type=NUMBER,
            Default='3008'
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

        params = {
            'FunctionName': f'{self._service_name(service.name)}Sync',
            'Code': Code(
                S3Bucket=self._bucket,
                S3Key=self._code_key
            ),
            'Handler': 'handlers.main',
            'Role': GetAtt(role_title, 'Arn'),
            'Runtime': 'python3.7',
            'MemorySize': Ref(memory_size),
            'Timeout': Ref(timeout_gateway),
            'Environment': self._lambda_environment(context)
        }
        if self._security_group_ids and self._subnet_ids:
            params['VpcConfig'] = VPCConfig(
                SecurityGroupIds=self._security_group_ids,
                SubnetIds=self._subnet_ids
            )
        api_lambda = template.add_resource(Function(
            f'{self._lambda_resource_name(service.name)}Sync',
            **params
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

        params = {
            'FunctionName': f'{self._service_name(service.name)}Async',
            'Code': Code(
                S3Bucket=self._bucket,
                S3Key=self._code_key
            ),
            'Handler': 'handlers.main',
            'Role': GetAtt(role_title, 'Arn'),
            'Runtime': 'python3.7',
            'MemorySize': Ref(memory_size),
            'Timeout': Ref(timeout_async),
            'Environment': self._lambda_environment(context)
        }
        if self._security_group_ids and self._subnet_ids:
            params['VpcConfig'] = VPCConfig(
                SecurityGroupIds=self._security_group_ids,
                SubnetIds=self._subnet_ids
            )
        async_lambda = template.add_resource(Function(
            f'{self._lambda_resource_name(service.name)}Async',
            **params
        ))

        # Timers
        for cls, _ in context.command_handlers.items():
            if cls.has_timer():
                timer = cls.get_timer()
                if timer.environment is not None and timer.environment != self._env:
                    continue
                if isinstance(timer.command, str):
                    timer_name = timer.command
                else:
                    timer_name = timer.command.__name__

                target = Target(
                    f'{self._service_name(service.name)}AsyncTarget',
                    Arn=GetAtt(f'{self._lambda_resource_name(service.name)}Async', 'Arn'),
                    Id=f'{self._lambda_resource_name(service.name)}Async',
                    Input=f'{{"_context": "{context.name}", "_type": "command", "_name": "{cls.__name__}"}}'
                )
                rule = template.add_resource(Rule(
                    f'{timer_name}TimerRule',
                    ScheduleExpression=f'cron({timer.cron})',
                    State='ENABLED',
                    Targets=[target]
                ))
                template.add_resource(Permission(
                    f'{timer_name}TimerPermission',
                    Action='lambda:invokeFunction',
                    Principal='events.amazonaws.com',
                    FunctionName=Ref(async_lambda),
                    SourceArn=GetAtt(rule, 'Arn')
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
            AuthorizationType='NONE',
            Target=Join('/', ['integrations', Ref(integration)]),
            DependsOn=integration
        ))

        template.add_resource(Route(
            f'{self._route_name(context.name)}Proxy',
            ApiId=ImportValue(self._rest_api_reference()),
            RouteKey=f'ANY /{proxy_route}',
            AuthorizationType='NONE',
            Target=Join('/', ['integrations', Ref(integration)]),
            DependsOn=integration
        ))

        # Error alarms / subscriptions

        if 'errors' in self._aws_config:
            alerts_topic = template.add_resource(Topic(
                self._alert_topic_name(service.name),
                TopicName=self._alert_topic_name(service.name)
            ))
            self._add_error_alarm(template, f'{self._service_name(context.name)}Sync', context.name, alerts_topic)
            self._add_error_alarm(template, f'{self._service_name(context.name)}Async', context.name, alerts_topic)

            if 'email' in self._aws_config.get('errors'):
                template.add_resource(SubscriptionResource(
                    self._alarm_subscription_name(context.name),
                    Protocol='email',
                    Endpoint=self._aws_config.get('errors').get('email').get('recipients'),
                    TopicArn=self._alert_topic_arn(context.name),
                    DependsOn=[alerts_topic]
                ))

        # Queues / Topics

        subscriptions = {}
        for subscription in self._get_subscriptions(context):
            if subscription['context'] not in subscriptions:
                subscriptions[subscription['context']] = []
            subscriptions[subscription['context']].append(subscription)

        dlq = template.add_resource(Queue(
            f'{self._queue_name(context.name)}Dlq',
            QueueName=f'{self._queue_name(context.name)}Dlq',
            VisibilityTimeout=905,
            ReceiveMessageWaitTimeSeconds=20,
            MessageRetentionPeriod=1209600
        ))
        self._queue_policy(template, dlq, f'{self._queue_name(context.name)}Dlq', subscriptions)

        queue = template.add_resource(Queue(
            self._queue_name(context.name),
            QueueName=self._queue_name(context.name),
            VisibilityTimeout=905,
            ReceiveMessageWaitTimeSeconds=20,
            MessageRetentionPeriod=1209600,
            RedrivePolicy=RedrivePolicy(
                deadLetterTargetArn=GetAtt(dlq, 'Arn'),
                maxReceiveCount=1000
            ),
            DependsOn=dlq
        ))
        self._queue_policy(template, queue, self._queue_name(context.name), subscriptions)

        template.add_resource(EventSourceMapping(
            f'{self._lambda_resource_name(context.name)}AsyncMapping',
            BatchSize=1,
            Enabled=True,
            EventSourceArn=GetAtt(queue, 'Arn'),
            FunctionName=f'{self._service_name(service.name)}Async',
            DependsOn=[queue, async_lambda]
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
                    TopicArn=self._topic_arn(context.name),
                    FilterPolicy={
                        '_name': [x['name'] for x in list_],
                    },
                    RedrivePolicy={
                        'deadLetterTargetArn': GetAtt(dlq, 'Arn'),
                    },
                    DependsOn=[queue, dlq, topic]
                ))
            elif len(list_) > 0:
                if context_name not in self._context_map.contexts:
                    self._find_or_create_topic(context_name)
                template.add_resource(SubscriptionResource(
                    self._subscription_name(context.name, context_name),
                    Protocol='sqs',
                    Endpoint=GetAtt(queue, 'Arn'),
                    TopicArn=self._topic_arn(context_name),
                    FilterPolicy={
                        '_name': [x['name'] for x in list_]
                    },
                    RedrivePolicy={
                        'deadLetterTargetArn': GetAtt(dlq, 'Arn'),
                    },
                    DependsOn=[queue, dlq]
                ))

        # DynamoDB Table

        ddb_table = template.add_resource(Table(
            self._ddb_resource_name(context.name),
            TableName=self._ddb_table_name(context.name),
            AttributeDefinitions=[
                AttributeDefinition(AttributeName='pk', AttributeType='S'),
                AttributeDefinition(AttributeName='sk', AttributeType='S'),
            ],
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                KeySchema(AttributeName='pk', KeyType='HASH'),
                KeySchema(AttributeName='sk', KeyType='RANGE'),
            ]
        ))

        template.add_output(Output(
            "DDBTable",
            Value=Ref(ddb_table),
            Description="Document table"
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

        self._migrate_schema(context)

        self.info('Done')

    def _migrate_schema(self, context: ff.Context):
        for entity in context.entities:
            if issubclass(entity, ff.AggregateRoot) and entity is not ff.AggregateRoot:
                try:
                    repository = self._registry(entity)
                    if isinstance(repository, ffi.RdbRepository):
                        repository.migrate_schema()
                except ff.FrameworkError:
                    self.debug('Could not execute ddl for entity %s', entity)

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
        subprocess.call([
            'pip', 'install',
            '-r', self._deployment.requirements_file or 'requirements.txt',
            '-t', './build/python-sources'
        ])

        self.info('Packaging artifact')
        subprocess.call(['cp', 'templates/aws/handlers.py', 'build/python-sources/.'])
        os.chdir('./build/python-sources')
        with open('firefly.yml', 'w') as fp:
            fp.write(yaml.dump(self._configuration.all))

        subprocess.call(['find', '.', '-name', '"*.so"', '-exec', 'strip', '{}', ';'])
        subprocess.call(['find', '.', '-name', '"*.so.*"', '-exec', 'strip', '{}', ';'])
        subprocess.call(['find', '.', '-name', '"*.pyc"', '-exec', 'rm', '-Rf', '{}', ';'])
        subprocess.call(['find', '.', '-name', 'tests', '-type', 'd', '-exec', 'rm', '-R', '{}', ';'])

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

        self._clean_up_old_artifacts(context)

    def _clean_up_old_artifacts(self, context: ff.Context):
        response = self._s3_client.list_objects(
            Bucket=self._bucket,
            Prefix=self._code_path
        )

        files = []
        for row in response['Contents']:
            files.append((row['Key'], row['LastModified']))
        if len(files) < 3:
            return

        files.sort(key=lambda i: i[1], reverse=True)
        for key, _ in files[2:]:
            self._s3_client.delete_object(Bucket=self._bucket, Key=key)

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
            Default='3008'
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
            DependsOn=[integration]
        ))

        template.add_resource(Stage(
            f'{self._rest_api_name()}Stage',
            StageName='v2',
            ApiId=Ref(api),
            AutoDeploy=True
        ))

        template.add_resource(Deployment(
            f'{self._rest_api_name()}Deployment',
            ApiId=Ref(api),
            StageName='v2',
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

    def _add_error_alarm(self, template, function_name: str, context: str, topic):
        template.add_resource(Alarm(
            f'{function_name}ErrorAlarm',
            AlarmActions=[self._alert_topic_arn(context)],
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=1,
            MetricName='Errors',
            Namespace='AWS/Lambda',
            Dimensions=[MetricDimension(Name='FunctionName', Value=function_name)],
            Period=60,
            Statistic='Sum',
            Threshold=0,
            DependsOn=[topic]
        ))

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
                                    'cloudfront:CreateInvalidation',
                                    'ec2:*NetworkInterface',
                                    'ec2:DescribeNetworkInterfaces',
                                    'lambda:InvokeFunction',
                                    'rds-data:*',
                                    's3:*',
                                    'secretsmanager:GetSecretValue',
                                    'sns:*',
                                    'sqs:*',
                                    'dynamodb:*',
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
        env = ((context.config.get('extensions') or {}).get('firefly_aws') or {}).get('environment')

        defaults = {
            'PROJECT': self._project,
            'ENV': self._env,
            'ACCOUNT_ID': self._account_id,
            'CONTEXT': context.name,
            'REGION': self._region,
            'BUCKET': self._bucket,
            'DDB_TABLE': self._ddb_table_name(context.name),
        }
        if env is not None:
            defaults.update(env)

        return Environment(
            'LambdaEnvironment',
            Variables=defaults
        )

    def _queue_policy(self, template: Template, queue, queue_name: str, subscriptions: dict):
        template.add_resource(QueuePolicy(
            f'{queue_name}Policy',
            Queues=[Ref(queue)],
            PolicyDocument={
                'Version': '2008-10-17',
                'Id': f'{queue_name}Policy',
                'Statement': [{
                    'Action': [
                        'sqs:SendMessage',
                    ],
                    'Effect': 'Allow',
                    'Resource': GetAtt(queue, 'Arn'),
                    'Principal': {
                        'AWS': '*',
                    },
                    'Condition': {
                        'ForAnyValue:ArnEquals': {
                            'aws:SourceArn': [
                                self._topic_arn(name) for name in subscriptions.keys()
                            ]
                        }
                    }
                }]
            },
            DependsOn=queue
        ))
