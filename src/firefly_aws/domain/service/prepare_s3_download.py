from __future__ import annotations

import firefly as ff


class PrepareS3Download(ff.DomainService):
    _s3_client = None

    def __call__(self, bucket: str, key: str):
        return self._s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key})
