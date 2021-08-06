from __future__ import annotations

import os
from statistics import stdev
from typing import Callable

import firefly as ff
import psutil
from botocore.exceptions import EndpointConnectionError
from firefly import domain as ffd

import firefly_aws.domain as domain


# TODO Make sure to set this env variable in the agent
if os.environ.get('ADAPTIVE_MEMORY'):
    @ff.register_middleware(index=1, buses=['event', 'command'])
    class ResourceMonitoringMiddleware(ff.Middleware):
        _resource_monitor: domain.ResourceMonitor = None
        _execution_context: domain.ExecutionContext = None
        _message_factory: ff.MessageFactory = None
        _message_transport: ff.MessageTransport = None
        _configuration: ff.Configuration = None
        _check_resource_usage: domain.CheckResourceUsage = None
        _context: str = None
        _memory_settings: list = None

        def __init__(self):
            context = self._configuration.contexts[self._context]
            if context.get('memory') == 'adaptive':
                self._memory_settings = sorted(list(map(int, context.get('memory_settings'))))
                if self._memory_settings is None:
                    raise ff.ConfigurationError(
                        'When using "adaptive" memory you must provide a list of memory_settings'
                    )

        def __call__(self, message: ffd.Message, next_: Callable) -> ffd.Message:
            response = None
            requeue = False

            try:
                response = next_(message)
            except (MemoryError, EndpointConnectionError):
                requeue = True
            finally:
                self._check_resource_usage(message, requeue)

            return response
