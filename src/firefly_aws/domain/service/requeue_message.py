from __future__ import annotations

import firefly as ff

import firefly_aws.domain as domain

TIME_LIMIT = 900_000


class RequeueMessage(ff.DomainService):
    _resource_monitor: domain.ResourceMonitor = None
    _execution_context: domain.ExecutionContext = None
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

    def __call__(self, message: ff.Message, **kwargs):
        if not self._execution_context.context:
            return

        memory_index = None
        memory_limit = self._execution_context.context.memory_limit_in_mb

        if self._memory_settings is not None:
            memory_index = self._memory_settings.index(int(memory_limit))

        if memory_index is None or memory_index >= (len(self._memory_settings) - 1):
            raise MemoryError()
        setattr(message, '_memory', self._memory_settings[memory_index + 1])

        if isinstance(message, ff.Event):
            self._message_transport.dispatch(message)
        elif isinstance(message, ff.Command):
            setattr(message, '_async', True)
            self._message_transport.invoke(message)
