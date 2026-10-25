

import json
import time
import argparse

#STAGE 1 and STAGE 2
class LogPipelineError(Exception): 
    """Base Exception Class"""
    pass

class MalformedJsonError(LogPipelineError): 
    """Data Extraction Failure Exception"""
    pass


class LogEntry:

     def __init__(self, raw_data: dict) -> None:
       self.timestamp = raw_data.get("timestamp", "UNKNOWN")
       self.level = raw_data.get("level", "INFO")
       self.service = raw_data.get("service", "GenericService")
       self.response_time = int(raw_data.get("response_time", 0))
       self.message = raw_data.get("message", "")


def stream_logs(file_path: str):
       start_time = time.perf_counter()
       with ManagedLogsStreamer(file_path) as streamer:
        for line in streamer.file_handle:
         cleaned_line = line.strip()
         if cleaned_line:
          try:
               parsed_dict = json.loads(cleaned_line)
               yield LogEntry(parsed_dict) 

          except json.JSONDecodeError:
              print(f"■ [SKIPPED] Found a broken log line: {cleaned_line}")
              continue
       duration = (time.perf_counter() - start_time) * 1000
       print(f"Log processing time: {duration:.3f} ms")

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

#STAGE-4

def non_negative_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "Latency must be an integer"
        ) from error

    if number < 0:
        raise argparse.ArgumentTypeError(
            "Latency cannot be negative"
        )

    return number
  
def setup_cli():
    parser = argparse.ArgumentParser(
        description="Stream and filter production logs"
    )

    parser.add_argument(
        "--file",
        required=True,
        help="Path to the JSON log file"
    )

    parser.add_argument(
        "--level",
        default=None,
        help="Filter by log level, such as ERROR"
    )

    parser.add_argument(
        "--latency",
        type=non_negative_int,
        default=0,
        help="Minimum response time in milliseconds"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = setup_cli()

    # Filter criteria from the command-line arguments
    runtime_conditions = lambda log: (
        (args.level is None or log.level == args.level)
        and (log.response_time >= args.latency)
    )

    try:
        for log in stream_logs(args.file):
            if runtime_conditions(log):
                print(
                    f"{log.timestamp} | "
                    f"{log.level} | "
                    f"{log.service} | "
                    f"{log.response_time} ms | "
                    f"{log.message}"
                )

    except FileNotFoundError:
        print(f"Error: File not found: {args.file}")

    except Exception as error:
        print(f"Error processing logs: {error}")
