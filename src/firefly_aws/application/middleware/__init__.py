import os


if os.environ.get('ADAPTIVE_MEMORY'):
    from .adaptive_memory_routing_middleware import AdaptiveMemoryRoutingMiddleware
    from .resource_monitoring_middleware import ResourceMonitoringMiddleware
