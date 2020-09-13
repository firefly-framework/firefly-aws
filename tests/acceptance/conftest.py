import os
from time import sleep

import pytest
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from firefly.infrastructure import RdbRepository
from firefly_test.iam import User
from firefly_test.iam.domain.entity.role import Role
from firefly_test.iam.domain.entity.scope import Scope
from firefly_test.todo import TodoList

os.environ['ENV'] = 'dev'

load_dotenv(dotenv_path=os.path.join(os.getcwd(), '../../.env'))
load_dotenv(dotenv_path=os.path.join(os.getcwd(), '../../.env.dev'))

services = {
    'rdb': {
        'connection': {
            'driver': 'data_api_mysql',
            'db_arn': os.environ['DB_ARN'],
            'db_secret_arn': os.environ['DB_SECRET_ARN'],
            'db_name': os.environ['DB_NAME'],
        }
    },
    'mapped': {
        'type': 'rdb',
        'connection': {
            'driver': 'data_api_mysql_mapped',
            'db_arn': os.environ['DB_ARN'],
            'db_secret_arn': os.environ['DB_SECRET_ARN'],
            'db_name': os.environ['DB_NAME'],
        }
    },
}


@pytest.fixture(scope="session")
def config():
    return {
        'project': 'firefly-aws',
        'provider': 'aws',
        'contexts': {
            'firefly_aws': {
                'region': '${AWS_DEFAULT_REGION}',
                'bucket': 'firefly-aws',
            },
            'todo': {
                'entity_module': 'firefly_test.todo.domain',
                'container_module': 'firefly_test.todo.application',
                'application_module': 'firefly_test.todo.application',
                'storage': {
                    'services': services,
                    'default': 'rdb',
                },
                'extensions': {
                    'firefly_aws': {
                        'environment': {}
                    }
                },
            },
            'iam': {
                'entity_module': 'firefly_test.iam.domain',
                'container_module': 'firefly_test.iam.application',
                'application_module': 'firefly_test.iam.application',
                'storage': {
                    'services': services,
                    'default': 'rdb',
                },
                'extensions': {
                    'firefly_aws': {
                        'environment': {}
                    }
                },
            },
            'calendar': {
                'entity_module': 'firefly_test.calendar.domain',
                'storage': {
                    'services': services,
                    'default': 'rdb',
                },
                'extensions': {
                    'firefly_aws': {
                        'environment': {}
                    }
                },
            },
        },
    }


@pytest.fixture(scope="session", autouse=True)
def wake_up_database(container):
    while True:
        try:
            container.data_api.execute('select 1 from dual')
            break
        except ClientError:
            sleep(5)


interfaces = ['data_api_mysql', 'data_api_mysql_mapped']


@pytest.fixture()
def todo_repositories(container):
    return build_repositories(TodoList, container)


@pytest.fixture()
def user_repositories(container):
    return build_repositories(User, container)


@pytest.fixture()
def role_repositories(container):
    return build_repositories(Role, container)


@pytest.fixture()
def scope_repositories(container):
    return build_repositories(Scope, container)


def build_repositories(entity, container):
    return {
        'data_api_mysql': build_repository(RdbRepository[entity], container, 'data_api_mysql'),
        'data_api_mysql_mapped': build_repository(RdbRepository[entity], container, 'data_api_mysql_mapped'),
    }


def build_repository(base, container, interface):
    class Repo(base):
        pass

    return container.build(
        Repo,
        interface=container.build(container.rdb_storage_interface_registry.get(interface))
    )


tables = ['todo.todo_lists', 'iam.users', 'iam.roles', 'iam.scopes']


@pytest.fixture(scope='function', autouse=True)
def drop_tables(container):
    for k, v in services.items():
        for table in tables:
            container.data_api.execute(
                f"drop table if exists {table}",
                db_arn=v['connection']['db_arn'],
                db_secret_arn=v['connection']['db_secret_arn'],
                db_name=v['connection']['db_name']
            )
