# CORE STREAM ENGINE

import json
class LogEntry:

     def __init__(self, raw_data: dict) -> None:
       self.timestamp = raw_data.get("timestamp", "UNKNOWN")
       self.level = raw_data.get("level", "INFO")
       self.service = raw_data.get("service", "GenericService")
       self.response_time = int(raw_data.get("response_time", 0))
       self.message = raw_data.get("message", "")


def stream_logs(file_path: str):
       with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
         cleaned_line = line.strip()
         if cleaned_line:
          parsed_dict = json.loads(cleaned_line)
          yield LogEntry(parsed_dict) 