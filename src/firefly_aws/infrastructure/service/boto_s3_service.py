#  Copyright (c) 2020 JD Williams
#
#  This file is part of Firefly, a Python SOA framework built by JD Williams. Firefly is free software; you can
#  redistribute it and/or modify it under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 3 of the License, or (at your option) any later version.
#
#  Firefly is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
#  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
#  Public License for more details. You should have received a copy of the GNU Lesser General Public
#  License along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  You should have received a copy of the GNU General Public License along with Firefly. If not, see
#  <http://www.gnu.org/licenses/>.

from __future__ import annotations

import firefly as ff
import firefly_aws.domain as awsd
from botocore.exceptions import ClientError


class BotoS3Service(awsd.S3Service, ff.LoggerAware):
    _configuration: ff.Configuration = None
    _s3_client = None

    def ensure_bucket_exists(self, bucket: str):
        try:
            self._s3_client.head_bucket(Bucket=bucket)
        except ClientError as e:
            if '404' in str(e):
                self.info(f"Bucket '{bucket}' does not exist. Creating it now.")
                self._s3_client.create_bucket(
                    ACL='private',
                    Bucket=bucket,
                    CreateBucketConfiguration={
                        'LocationConstraint': self._configuration.contexts.get('firefly_aws').get('region'),
                    }
                )
