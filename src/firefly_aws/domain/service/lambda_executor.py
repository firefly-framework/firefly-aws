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

from typing import Union

import firefly as ff


class LambdaExecutor(ff.DomainService, ff.SystemBusAware, ff.LoggerAware):
    _serializer: ff.Serializer = None
    _message_factory: ff.MessageFactory = None

    def run(self, event: dict, context: dict):
        print(event)
        print(context)
        return event
        # if 'httpMethod' in event:
        #     return self._handle_http_event(event)
        # elif 'Records' in event and 'aws:sqs' == event['Records'][0].get('eventSource'):
        #     self._handle_sqs_event(event)

    def _handle_http_event(self, event: dict):
        body = self._serializer.deserialize(event['body'])
        if event['httpMethod'].lower() == 'get':
            message = self._serializer.deserialize(event['queryStringParameters']['query'])
        else:
            message = self._serializer.deserialize(body)

        message.headers['http_request'] = {
            'headers': event['headers'],
            'method': event['httpMethod'],
            # 'path': request.path,
            # 'content_type': request.content_type,
            # 'content_length': request.content_length,
            # 'query': dict(request.query),
            # 'post': dict(await request.post()),
            # 'url': request._message.url,
        }

        if isinstance(message, ff.Command):
            response = self.invoke(message)
        else:
            response = self.request(message)

        if isinstance(response, ff.HttpResponse):
            body = response.body
            headers = response.headers
        else:
            body = self._serializer.serialize(response)
            headers = {}

        return {
            'isBase64Encoded': False,
            'statusCode': 200,
            'headers': headers,
            'body': body
        }

    def _handle_sqs_event(self, event: dict):
        for record in event['Records']:
            body = self._serializer.deserialize(record['body'])
            message: Union[ff.Event, dict] = self._serializer.deserialize(body['Message'])

            if 'PAYLOAD_KEY' in message:
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
        return {}

    def nack_message(self, record: dict):
        pass

    def complete_handshake(self, record: dict):
        pass

    def complete_batch_handshake(self, records: list):
        pass
