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

import uuid
from typing import Any, Union

import firefly as ff
import firefly_aws as domain
from botocore.exceptions import ClientError
from firefly import Query, Command, Event


class BotoMessageTransport(ff.MessageTransport, domain.ResourceNameAware):
    _serializer: ff.Serializer = None
    _lambda_client = None
    _sns_client = None
    _s3_client = None
    _bucket: str = None

    def dispatch(self, event: Event) -> None:
        try:
            self._sns_client.publish(
                TopicArn=self._topic_arn(event.get_context()),
                Message=self._store_large_payloads_in_s3(self._serializer.serialize(event)),
                MessageAttributes={
                    '_name': {
                        'DataType': 'String',
                        'StringValue': event.__class__.__name__,
                    },
                    '_type': {
                        'DataType': 'String',
                        'StringValue': 'event'
                    },
                    '_context': {
                        'DataType': 'String',
                        'StringValue': event.get_context()
                    },
                }
            )
        except ClientError as e:
            raise ff.MessageBusError(str(e))

    def invoke(self, command: Command) -> Any:
        return self._invoke_lambda(command)

    def request(self, query: Query) -> Any:
        return self._invoke_lambda(query)

    def _invoke_lambda(self, message: Union[Command, Query]):
        async_ = False
        if hasattr(message, '_async'):
            async_ = getattr(message, '_async')

        try:
            response = self._lambda_client.invoke(
                FunctionName=f'{self._service_name(message.get_context())}Sync',
                InvocationType='RequestResponse' if async_ is False else 'Event',
                LogType='None',
                Payload=self._serializer.serialize(message)
            )
        except ClientError as e:
            raise ff.MessageBusError(str(e))

        return self._serializer.deserialize(response['Payload'].read().decode('utf-8'))

    def _store_large_payloads_in_s3(self, payload: str):
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
