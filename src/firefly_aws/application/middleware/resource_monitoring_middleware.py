from __future__ import annotations

import json
import os
import resource
from typing import Callable

import firefly as ff
import psutil
from botocore.exceptions import EndpointConnectionError
from firefly import domain as ffd

import firefly_aws.domain as domain

TIME_LIMIT = 900_000


if os.environ.get('ADAPTIVE_MEMORY'):
    # @ff.register_middleware(index=1, buses=['event', 'command'])
    class ResourceMonitoringMiddleware(ff.Middleware, domain.ResourceNameAware, ff.LoggerAware):
        _resource_monitor: domain.ResourceMonitor = None
        _execution_context: domain.ExecutionContext = None
        _message_factory: ff.MessageFactory = None
        _message_transport: ff.MessageTransport = None
        _configuration: ff.Configuration = None
        _requeue_message: domain.RequeueMessage = None
        _kinesis_client = None
        _context: str = None
        _memory_settings: list = None

        def __init__(self):
            context = self._configuration.contexts['firefly_aws']
            if context.get('memory_async') == 'adaptive':
                self._memory_settings = sorted(list(map(int, context.get('memory_settings'))))
                if self._memory_settings is None:
                    raise ff.ConfigurationError(
                        'When using "adaptive" memory you must provide a list of memory_settings'
                    )
                # self._set_soft_memory_limit()

        def __call__(self, message: ffd.Message, next_: Callable) -> ffd.Message:
            response = None

            try:
                response = next_(message)

                if isinstance(message, ff.Event) or (isinstance(message, ff.Command) and hasattr(message, '_async')):
                    memory_limit = self._execution_context.context.memory_limit_in_mb
                    memory_index = self._memory_settings.index(int(memory_limit))
                    memory_percent_used = psutil.Process(os.getpid()).memory_percent() / 100
                    memory_used = memory_percent_used * float(memory_limit)
                    self._kinesis_client.put_record(
                        StreamName=self._stream_resource_name(self._context),
                        Data=json.dumps({
                            'event_type': 'resource-usage',
                            'message': str(message),
                            'memory_used': float(memory_used),
                            'run_time': float(
                                TIME_LIMIT - self._execution_context.context.get_remaining_time_in_millis()
                            ),
                            'max_memory': float(memory_limit),
                            'prev_memory_tier': float(self._memory_settings[memory_index - 1])
                            if memory_index > 0 else None,
                        }).encode('utf-8'),
                        PartitionKey='resource-monitor'
                    )
            except (MemoryError, EndpointConnectionError, SystemExit):
                self._requeue_message(message)
            except AttributeError:
                pass

            return response

        def _set_soft_memory_limit(self):
            if not self._execution_context.context:
                return

            limit = int(self._execution_context.context.memory_limit_in_mb * 1048576 * .9)
            self.info(f'Setting soft memory limit to {limit} bytes')
            _, hard = resource.getrlimit(resource.RLIMIT_AS)
            resource.setrlimit(resource.RLIMIT_AS, (limit, hard))
