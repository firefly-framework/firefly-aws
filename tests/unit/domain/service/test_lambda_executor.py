import pytest
import random
import string
import re

from unittest import mock
from firefly_aws.domain import LambdaExecutor
from firefly_aws.application import Container
import firefly.infrastructure as ff_infra


@pytest.fixture()
def sut():
    ret = Container().mock(LambdaExecutor)
    ret._serializer = ff_infra.JsonSerializer()

    return ret

def make_body(size):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))

def test_handle_http_response_body_upper_limit_calls_s3_service_and_changes_location_url(sut):
    UPPER_LIMIT = 10000001
    FAKE_DOMAIN = 'www.fake-domain.com'
    sut._serializer.serialize = mock.MagicMock()
    sut._serializer.serialize.return_value = make_body(UPPER_LIMIT)
    sut._s3_service.store_download = mock.MagicMock()
    sut._s3_service.store_download.return_value = 'https://fake-s3-bucket.amazonaws.com//tmp/fake-pre-signed-url'
    sut._configuration.environments = {
        'S3_DOMAIN_URL': FAKE_DOMAIN
    }

    ret = sut._handle_http_response({})

    sut._serializer.serialize.assert_called()
    sut._s3_service.store_download.assert_called()
    assert 'body' in ret
    assert 'headers' in ret
    assert 'location' in ret.get('body')
    assert 'Location' in ret.get('headers')
    assert FAKE_DOMAIN in ret.get('headers').get('Location')
