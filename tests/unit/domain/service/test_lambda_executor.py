import pytest
from firefly_aws.domain import LambdaExecutor
from firefly_aws.application import Container
import firefly.infrastructure as ff_infra


@pytest.fixture()
def sut():
    ret = Container().mock(LambdaExecutor)
    ret._serializer = ff_infra.JsonSerializer()

    return ret
