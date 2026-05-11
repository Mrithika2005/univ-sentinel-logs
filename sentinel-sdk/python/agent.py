import json
import time
import uuid
import datetime
import sys
import os
import functools

class LogLayer:
    PRESENTATION = 'presentation'
    API_GATEWAY = 'api_gateway'
    BUSINESS_LOGIC = 'business_logic'
    DATA_ACCESS = 'data_access'
    SERVICE = 'service'
    SECURITY = 'security'
    OBSERVABILITY = 'observability'
    INFRASTRUCTURE = 'infrastructure'

class LogLevel:
    INFO = 'INFO'
    WARN = 'WARN'
    ERROR = 'ERROR'
    DEBUG = 'DEBUG'
    FATAL = 'FATAL'

class LogRecord:
    def __init__(self, message, layer=LogLayer.BUSINESS_LOGIC, level=LogLevel.INFO, service='unknown-python-service', context=None):
        self.message = message
        self.layer = layer
        self.level = level
        self.timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.record_id = str(uuid.uuid4())
        self.trace_id = 'untracked'
        self.span_id = 'untracked'
        self.service = service
        self.env = os.getenv('ENV', 'development')
        self.context = context or {}

    def to_dict(self):
        return self.__dict__

    def __str__(self):
        color = "\033[92m" # Green
        if self.level == LogLevel.ERROR: color = "\033[91m" # Red
        elif self.level == LogLevel.WARN: color = "\033[93m" # Yellow
        reset = "\033[0m"
        return f"{color}[{self.timestamp}] [{self.layer.upper()}] [{self.level}] {self.message}{reset}"

class SentinelPython:
    def __init__(self, service_name='python-service'):
        self.service_name = service_name

    def hook(self):
        self._patch_builtins()
        print(LogRecord("Sentinel Python Agent hooked successfully", layer=LogLayer.INFRASTRUCTURE, service=self.service_name))

    def _patch_builtins(self):
        # Patch print to intercept logs
        original_print = print
        def sentinel_print(*args, **kwargs):
            msg = " ".join(map(str, args))
            if "[SENTINEL]" in msg:
                original_print(*args, **kwargs)
                return
            
            record = LogRecord(msg, service=self.service_name)
            original_print(f"[SENTINEL] {record}")
        
        # In a real library, we'd use a more sophisticated approach like sys.modules level patching
        import builtins
        builtins.print = sentinel_print

    def track_function(self, layer=LogLayer.BUSINESS_LOGIC):
        """Decorator to track function execution"""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                record = LogRecord(f"Calling {func.__name__}", layer=layer, service=self.service_name)
                print(f"[SENTINEL] {record}")
                try:
                    result = func(*args, **kwargs)
                    duration = (time.time() - start_time) * 1000
                    success_record = LogRecord(f"Finished {func.__name__} in {duration:.2f}ms", layer=layer, service=self.service_name)
                    print(f"[SENTINEL] {success_record}")
                    return result
                except Exception as e:
                    err_record = LogRecord(f"Error in {func.__name__}: {str(e)}", layer=layer, level=LogLevel.ERROR, service=self.service_name)
                    print(f"[SENTINEL] {err_record}")
                    raise
            return wrapper
        return decorator

def init_sentinel(name='python-service'):
    s = SentinelPython(name)
    s.hook()
    return s
