from __future__ import annotations

import base64
import binascii

import firefly as ff


class S3FileSystem(ff.FileSystem, ff.LoggerAware):
    _s3_client = None
    _bucket: str = None

    def read(self, file_name: str) -> ff.File:
        bucket, file_name = self._parse_file_path(file_name)
        response = self._s3_client.get_object(
            Bucket=bucket,
            Key=file_name
        )
        return ff.File(
            name=file_name,
            content=response['Body'].read().decode('utf-8'),
            content_type=response.get('ContentType', None)
        )

    def write(self, file: ff.File, path: str = None):
        path = '/'.join([(path or '').rstrip('/'), file.name])
        bucket, file_name = self._parse_file_path(path)
        params = {}
        if file.content_type is not None:
            params['ContentType'] = file.content_type
        self._s3_client.put_object(
            Bucket=bucket,
            Key=file_name,
            Body=file.content,
            **params
        )

    def _parse_file_path(self, path: str):
        parts = path.lstrip('/').split('/')
        bucket = parts.pop(0)
        file_name = '/'.join(parts)

        return bucket, file_name
