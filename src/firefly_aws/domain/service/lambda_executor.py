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

import re
from typing import Union

import firefly as ff


class LambdaExecutor(ff.DomainService, ff.SystemBusAware, ff.LoggerAware):
    _serializer: ff.Serializer = None
    _message_factory: ff.MessageFactory = None
    _rest_router: ff.RestRouter = None
    _s3_client = None
    _bucket: str = None

    def __init__(self):
        self._version_matcher = re.compile(r'^/v\d')

    def run(self, event: dict, context: dict):
        print(event)
        print(context)
        if 'requestContext' in event and 'http' in event['requestContext']:
            self.info('HTTP request')
            return self._handle_http_event(event)

        if 'Records' in event and 'aws:sqs' == event['Records'][0].get('eventSource'):
            self.info('SQS message')
            self._handle_sqs_event(event)

        return event

    def _handle_http_event(self, event: dict):
        body = self._serializer.deserialize(event['body']) if 'body' in event else None
        route = self._version_matcher.sub('', event['rawPath'])
        method = event['requestContext']['http']['method']

        try:
            self.info(f'Trying to match route: "{method} {route}"')
            message_name, params = self._rest_router.match(route, method)
            self.info(f'Matched route')
            params['headers'] = {
                'http_request': {
                    'headers': event['headers'],
                }
            }
            if method.lower() == 'get':
                return self.request(message_name, data=params)
            else:
                if body is not None:
                    params.update(body)
                return self.invoke(message_name, params)
        except TypeError:
            pass

    def _handle_sqs_event(self, event: dict):
        for record in event['Records']:
            body = self._serializer.deserialize(record['body'])
            message: Union[ff.Event, dict] = self._serializer.deserialize(body['Message'])

            if isinstance(message, dict) and 'PAYLOAD_KEY' in message:
                try:
                    message = self.load_payload(message['PAYLOAD_KEY'])
                except Exception as e:
                    self.nack_message(record)
                    self.error(e)
                    continue

            self.dispatch(message)
        if len(event['Records']) == 1:
            self.complete_handshake(event['Records'][0])
        else:
            self.complete_batch_handshake(event['Records'])

    def load_payload(self, key: str):
        return self._serializer.deserialize(
            self._s3_client.get_object(
                Bucket=self._bucket,
                Key=key
            )
        )

    def nack_message(self, record: dict):
        pass

    def complete_handshake(self, record: dict):
        pass

    def complete_batch_handshake(self, records: list):
        pass
