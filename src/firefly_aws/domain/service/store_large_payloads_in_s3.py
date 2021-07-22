from __future__ import annotations

import uuid

import firefly as ff


class StoreLargePayloadsInS3(ff.DomainService):
    _s3_client = None
    _serializer: ff.Serializer = None
    _bucket: str = None

    def __call__(self, payload: str):
        if len(payload) > 64_000:
            key = f'tmp/{str(uuid.uuid1())}.json'
            self._s3_client.put_object(
                Body=payload,
                Bucket=self._bucket,
                Key=key
            )
            return self._serializer.serialize({
                'PAYLOAD_KEY': key,
            })

        return payload
