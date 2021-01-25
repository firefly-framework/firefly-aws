from __future__ import annotations

import firefly as ff


class S3FileSystem(ff.FileSystem):
    _s3_client = None
    _bucket: str = None

    def read(self, file_name: str):
        bucket, file_name = self._parse_file_path(file_name)
        response = self._s3_client.get_object(
            Bucket=bucket,
            Key=file_name
        )
        return response['Body'].read().decode('utf-8')

    def write(self, file_name: str, data):
        bucket, file_name = self._parse_file_path(file_name)
        self._s3_client.put_object(
            Bucket=bucket,
            Key=file_name,
            Body=data
        )

    def _parse_file_path(self, path: str):
        parts = path.lstrip('/').split('/')
        bucket = parts.pop(0)
        file_name = '/'.join(parts)

        return bucket, file_name
