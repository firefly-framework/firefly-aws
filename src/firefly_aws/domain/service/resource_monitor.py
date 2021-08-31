from __future__ import annotations

from abc import ABC, abstractmethod


class ResourceMonitor(ABC):
    @abstractmethod
    def record_execution(self, message: str, seconds: float, memory: float):
        pass

    @abstractmethod
    def get_execution_metrics(self, message: str):
        pass

    @abstractmethod
    def set_memory_level(self, message: str, memory: int):
        pass

    @abstractmethod
    def get_memory_level(self, message: str):
        pass
