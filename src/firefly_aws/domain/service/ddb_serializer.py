from __future__ import annotations

import firefly as ff
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer


class DdbDeserializer(ff.DomainService):
    _serializer: TypeSerializer = None
    _deserializer: TypeDeserializer = None

    def __init__(self):
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    def serialize(self, data):
        return self._serializer.serialize(data)

    def deserialize(self, data: dict):
        return {
            k: self._deserializer.deserialize(v)
            for k, v in data.items()
        }
