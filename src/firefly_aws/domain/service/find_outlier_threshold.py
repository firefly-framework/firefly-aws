from __future__ import annotations

from math import floor
from statistics import median
from typing import List

import firefly as ff


class FindOutlierThreshold(ff.DomainService):
    
    def __call__(self, memory_list: List[int]):
        sorted_memory_usage = sorted(memory_list)
        median_memory_usage = median(sorted_memory_usage)
        absolute_deviation_from_median = [abs(value - median_memory_usage) for value in sorted_memory_usage]
        median_absolute_deviation = median(absolute_deviation_from_median)
        outlier_threshold = 0
        
        # If more than 50% of our values are the same value, MAD will be 0. Then initiate backup plan with z_score
        # (very rare edge-case) Not the best case to use z-score, but good backup
        if median_absolute_deviation == 0:
            # Outlier Threshold at 99.5% of runs if MAD is 0
            ninety_nine_five_threshold = floor(0.995 * len(sorted_memory_usage))
            outlier_threshold = sorted_memory_usage[ninety_nine_five_threshold]
        else:
            # 1.4826 is a constant linked to assumption of normally distributed data excluding outliers
            memory_mad = float(median_absolute_deviation) * 1.4826

            for value in sorted_memory_usage:
                memory_mad_relative_deviation = float(value - median_memory_usage) / memory_mad
                # Any relative deviation greater than 3.0 is an outlier
                if abs(memory_mad_relative_deviation) < 3.0:
                    # since we're iterating through a sorted list, the value can always be updated if true
                    outlier_threshold = value

        return outlier_threshold
