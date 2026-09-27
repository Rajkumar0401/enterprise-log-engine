

import json
import time
from functools import wraps
from typing import Callable, Any

class LogPipelineError(Exception): 
    """Base Exception Class"""
    pass

class MalformedJsonError(LogPipelineError): 
    """Data Extraction Failure Exception"""
    pass

def profile_performance(func: Callable) -> Callable:
    """Metaprogramming wrapper intercepting execution to log telemetries."""
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        duration = (time.perf_counter() - start_time) * 1000
        print(f"■■ [TELEMETRY] '{func.__name__}' evaluated in {duration:.3f} ms")
        return result
    return wrapper


class LogEntry:

     def __init__(self, raw_data: dict) -> None:
       self.timestamp = raw_data.get("timestamp", "UNKNOWN")
       self.level = raw_data.get("level", "INFO")
       self.service = raw_data.get("service", "GenericService")
       self.response_time = int(raw_data.get("response_time", 0))
       self.message = raw_data.get("message", "")

@profile_performance
def stream_logs(file_path: str):
       with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
         cleaned_line = line.strip()
         if cleaned_line:
          try:
               parsed_dict = json.loads(cleaned_line)
               yield LogEntry(parsed_dict) 

          except json.JSONDecodeError:
              print(f"■ [SKIPPED] Found a broken log line: {cleaned_line}")
              continue

# STAGE-3
class ManagedLogsStreamer:

    def __init__(self,file_path:str)->None:
        self.file_path=file_path
        self.file_handle=None

    def __enter__(self):
        # Allocating system handle:
        self.file_handle=open(self.file_path,"r",encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
       # releasing resource file handle cleanly
       if self.file_handle:
           self.file_handle.close()

       return False