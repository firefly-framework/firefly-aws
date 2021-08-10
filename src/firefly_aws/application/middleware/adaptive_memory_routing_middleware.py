from __future__ import annotations

import os
from functools import lru_cache
from typing import Callable

import firefly as ff

from ...domain.resource_name_aware import ResourceNameAware
from ...domain.service.execution_context import ExecutionContext
from ...domain.service.resource_monitor import ResourceMonitor

if os.environ.get('ADAPTIVE_MEMORY'):
    @ff.register_middleware(index=0, buses=['event', 'command'])
    class AdaptiveMemoryRoutingMiddleware(ff.Middleware, ResourceNameAware):
        _resource_monitor: ResourceMonitor = None
        _execution_context: ExecutionContext = None
        _message_transport: ff.MessageTransport = None
        _configuration: ff.Configuration = None
        _context: str = None

        def __init__(self):
            context = self._configuration.contexts['firefly_aws']
            if context.get('memory_async') == 'adaptive':
                self._memory_settings = sorted(list(map(int, context.get('memory_settings'))))
                if self._memory_settings is None:
                    raise ff.ConfigurationError(
                        'When using "adaptive" memory you must provide a list of memory_settings'
                    )

        def __call__(self, message: ff.Message, next_: Callable) -> ff.Message:
            print("1")
            function_name = self._lambda_function_name(self._context, 'Async')
            print("2")
            if self._execution_context.context and self._execution_context.context.function_name == function_name:
                print("3")
                if not hasattr(message, '_memory'):
                    print("4")
                    memory = self._get_memory_level(str(message))
                    print("5")
                    if memory is None:
                        print("6")
                        setattr(message, '_memory', str(self._memory_settings[0]))
                        print("7")
                        self._get_memory_level.cache_clear()
                        print("8")
                    else:
                        print("9")
                        setattr(message, '_memory', self._get_memory_level(str(message)))
                        print("10")
                print("11")
                self._enqueue_message(message)
                print("12")

                return message

            else:
                return next_(message)

        def _enqueue_message(self, message: ff.Message):
            if isinstance(message, ff.Event):
                print("13")
                self._message_transport.dispatch(message)
            elif isinstance(message, ff.Command):
                print("14")
                self._message_transport.invoke(message)

        @lru_cache(maxsize=None)
        def _get_memory_level(self, message: str):
            return self._resource_monitor.get_memory_level(message)
