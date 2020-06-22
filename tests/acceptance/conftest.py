import os
from time import sleep

import pytest
from botocore.exceptions import ClientError
from dotenv import load_dotenv

os.environ['ENV'] = 'dev'

load_dotenv(dotenv_path=os.path.join(os.getcwd(), '../../.env'))
load_dotenv(dotenv_path=os.path.join(os.getcwd(), '../../.env.dev'))


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
                    'services': {
                        'rdb': {
                            'connection': {
                                'driver': 'data_api_mysql',
                            }
                        },
                    },
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
                    'services': {
                        'rdb': {
                            'connection': {
                                'driver': 'data_api_mysql',
                            }
                        },
                    },
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
                    'services': {
                        'rdb': {
                            'connection': {
                                'driver': 'data_api_mysql',
                            }
                        },
                    },
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
