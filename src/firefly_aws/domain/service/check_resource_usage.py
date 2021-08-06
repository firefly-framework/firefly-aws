from __future__ import annotations

import os
from statistics import stdev, median

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

        # Want to check for requeue early as we don't want to write an execution until it succeeds
        if requeue is True:
            if memory_index is None or memory_index >= (len(self._memory_settings) - 1):
                raise MemoryError()
            setattr(message, '_memory', self._memory_settings[memory_index + 1])

            if isinstance(message, ff.Event):
                self._message_transport.dispatch(message)
            elif isinstance(message, ff.Command):
                setattr(message, '_async', True)
                self._message_transport.invoke(message)

            # Short circuit as to not write memory/time to DB for a failure
            return

        memory_percent_used = psutil.Process(os.getpid()).memory_percent()
        memory_used = memory_percent_used * memory_limit
        stats = self._resource_monitor.record_execution(
            str(message),
            TIME_LIMIT - self._execution_context.context.get_remaining_time_in_millis(),
            memory_used
        )

        # Using 2.58 as this deviation equates to 99.5%
        outlier_threshold = self._find_outlier_threshold(stats['memory'], 2.58)

        if outlier_threshold > (memory_limit * .9): # If threshold is greater than 90% of memory_limit, then bump up memory tier
            if (len(self._memory_settings) - 1) < memory_index:
                self._resource_monitor.set_memory_level(str(message), self._memory_settings[memory_index + 1])

        elif self._memory_settings is not None and memory_index > 0:
            memory_index -= 1
            lower_max = self._memory_settings[memory_index]
            if outlier_threshold < (lower_max * .9): # If threshold is less than 90% of lower_max, then safe to drop down memory tier
                self._resource_monitor.set_memory_level(str(message), self._memory_settings[memory_index])
    
    def _find_outlier_threshold(self, memory_list, deviation):
        sorted_memory_usage = sorted(memory_list)
        median_memory_usage = median(sorted_memory_usage)

        absolute_deviation_from_median = [abs(value - median_memory_usage) for value in sorted_memory_usage]
        
        median_absolute_deviation = median(absolute_deviation_from_median)

        outlier_threshold = 0
        
        # If more than 50% of our values are the same value, MAD will be 0. Then initiate backup plan with z_score (very rare edgecase)
        # Not the best case to use z-score, but good backup
        if median_absolute_deviation == 0:
            outlier_threshold = self._find_z_score_threshold(memory_list, deviation)
        else:
            # 1.4826 is a constant linked to assumption of normally distributed data excluding outliers
            memory_mad = median_absolute_deviation * 1.4826

            for value in sorted_memory_usage:
                memory_mad_relative_deviation = (value - median_memory_usage) / memory_mad
                # Any relative deviation greater than passed in deviation is an outlier
                if abs(memory_mad_relative_deviation) < deviation:
                    #since we're iterating through a sorted list, the value can always be updated if true
                    outlier_threshold = value

        return outlier_threshold

    def _find_z_score_threshold(self, memory_list, deviation):
        standard_deviation = stdev(memory_list)
        average = sum(memory_list) / len(memory_list)
        outlier_threshold = (standard_deviation * deviation) + average

        return outlier_threshold
