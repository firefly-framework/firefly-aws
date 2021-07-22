from __future__ import annotations

import bz2

import firefly as ff


class LoadPayload(ff.DomainService):
    _s3_client = None
    _serializer: ff.Serializer = None
    _bucket: str = None

    def __call__(self, key: str):
        response = self._s3_client.get_object(
            Bucket=self._bucket,
            Key=key
        )
        data = response['Body'].read()
        if key.endswith('bz2'):
            data = bz2.decompress(data)
        return self._serializer.deserialize(data)
