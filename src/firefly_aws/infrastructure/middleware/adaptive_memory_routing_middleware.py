from __future__ import annotations

import os
from functools import cache
from typing import Callable

import firefly as ff
from firefly import domain as ffd

import firefly_aws.domain as domain

if os.environ.get('ADAPTIVE_MEMORY'):
    @ff.register_middleware(index=0, buses=['event', 'command'])
    class AdaptiveMemoryRoutingMiddleware(ff.Middleware, domain.ResourceNameAware):
        _resource_monitor: domain.ResourceMonitor = None
        _execution_context: domain.ExecutionContext = None
        _context: str = None

        def __call__(self, message: ffd.Message, next_: Callable) -> ffd.Message:
            function_name = self._lambda_function_name(self._context, 'Async')
            if self._execution_context.context.function_name == function_name:
                if not hasattr(message, '_memory'):
                    setattr(message, '_memory', self._get_memory_level(str(message)))
                self._enqueue_message(message)

                return message

            else:
                return next_(message)

        def _enqueue_message(self, message: ff.Message):
            if isinstance(message, ff.Event):
                self.dispatch(message)
            elif isinstance(message, ff.Command):
                self.invoke(message)

        @cache
        def _get_memory_level(self, message: str):
            return self._resource_monitor.get_memory_level(message)
