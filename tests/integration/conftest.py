import os

import pytest
from firefly import infrastructure

infrastructure.YamlConfigurationFactory()()
os.environ['FF_ENVIRONMENT'] = 'dev'


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
        },
    }
