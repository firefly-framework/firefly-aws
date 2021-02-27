from __future__ import annotations

import traceback

import firefly as ff
import requests

import firefly_aws.domain as domain


class HandleError(ff.DomainService, domain.ResourceNameAware):
    _sns_client = None
    _serializer: ff.Serializer = None
    _slack_error_url: str = None
    _context: str = None

    def __call__(self, exception: Exception, event: dict, context):
        tb: str = traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)
        msg: str = self._build_message(exception, tb, event, context)

        if self._slack_error_url is not None:
            requests.post(self._slack_error_url, json={
                'text': msg,
            }, headers={
                'Content-Type': 'application/json',
            })

        self._sns_client.publish(
            TopicArn=self._alert_topic_arn(self._context),
            Message=self._serializer.serialize({
                'default': msg
            }),
            Subject=f'Error Executing {context.function_name}',
            MessageStructure='json'
        )

    def _build_message(self, exception: Exception, tb: list, event: dict, context):
        trace = "\n".join(tb)
        return f"""
Error Executing {context.function_name}
            
Got exception {exception.__class__.__name__}
            
Log Group: {context.log_group_name}
Log Stream: {context.log_stream_name}
Client Context: {context.client_context}

Event: {event}

Stack Trace:

{trace}
        """
