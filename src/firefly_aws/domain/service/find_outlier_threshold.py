from __future__ import annotations

from statistics import stdev, median
from typing import List

import firefly as ff

class FindOutlierThreshold(ff.DomainService):
    
    def __call__(self, memory_list: List[int], deviation: int):
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

    def _find_z_score_threshold(self, memory_list: List[int], deviation: int):
        standard_deviation = stdev(memory_list)
        average = sum(memory_list) / len(memory_list)
        outlier_threshold = (standard_deviation * deviation) + average

        return outlier_threshold
