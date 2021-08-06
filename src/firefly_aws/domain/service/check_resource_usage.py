from __future__ import annotations

import os
from statistics import stdev

import firefly as ff
import psutil

import firefly_aws.domain as domain

TIME_LIMIT = 900_000


class CheckResourceUsage(ff.DomainService):
    _resource_monitor: domain.ResourceMonitor = None
    _execution_context: domain.ExecutionContext = None
    _message_transport: ff.MessageTransport = None
    _configuration: ff.Configuration = None
    _context: str = None

    def __init__(self):
        context = self._configuration.contexts[self._context]
        if context.get('memory') == 'adaptive':
            self._memory_settings = sorted(list(map(int, context.get('memory_settings'))))
            if self._memory_settings is None:
                raise ff.ConfigurationError(
                    'When using "adaptive" memory you must provide a list of memory_settings'
                )

    def __call__(self, message: ff.Message, requeue: bool, **kwargs):
        memory_index = None
        memory_limit = self._execution_context.context.memory_limit_in_mb

        if self._memory_settings is not None:
            memory_index = self._memory_settings.index(memory_limit)

        memory_percent_used = psutil.Process(os.getpid()).memory_percent()
        memory_used = memory_percent_used * memory_limit
        stats = self._resource_monitor.record_execution(
            str(message),
            TIME_LIMIT - self._execution_context.context.get_remaining_time_in_millis(),
            memory_used
        )
        standard_deviation = stdev(stats['memory'])
        mean = sum(stats['memory']) / len(stats['memory'])
        outlier_threshold = (standard_deviation * 2.58) + mean

        if memory_used > outlier_threshold or requeue is True:
            if memory_index is None or memory_index >= (len(self._memory_settings) - 1):
                raise MemoryError()
            setattr(message, '_memory', self._memory_settings[memory_index + 1])

            if isinstance(message, ff.Event):
                self._message_transport.dispatch(message)
            elif isinstance(message, ff.Command):
                setattr(message, '_async', True)
                self._message_transport.invoke(message)

        elif outlier_threshold > (memory_limit * .9):
            if (len(self._memory_settings) - 1) < memory_index:
                self._resource_monitor.set_memory_level(str(message), self._memory_settings[memory_index + 1])

        elif self._memory_settings is not None and memory_index > 0:
            memory_index -= 1
            lower_max = self._memory_settings[memory_index]
            if outlier_threshold > (lower_max * .9):
                self._resource_monitor.set_memory_level(str(message), self._memory_settings[memory_index])
