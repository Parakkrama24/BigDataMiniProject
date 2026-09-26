import json
import logging

# Attribute names every LogRecord has by default. Anything else on a record
# was passed in through extra={...} by the caller.
_STANDARD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys())
_STANDARD_ATTRS |= {"message", "stage", "trace_id"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": record.created,
            "level": record.levelname,
            "message": record.getMessage(),
            "stage": getattr(record, "stage", None),
            "trace_id": getattr(record, "trace_id", None),
        }

        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS:
                log_entry[key] = value

        return json.dumps(log_entry, default=str)


class StageFilter(logging.Filter):
    def __init__(self, stage: str):
        super().__init__()
        self.stage = stage

    def filter(self, record: logging.LogRecord) -> bool:
        record.stage = self.stage
        return True

def get_logger(stage: str) -> logging.Logger:
    logger = logging.getLogger(f"bigdata.{stage}")

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.addFilter(StageFilter(stage))
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger
