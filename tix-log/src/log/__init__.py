"""
Structured logging without process-global configuration.
"""

from __future__ import annotations

from .formatters import JsonFormatter, redact
from .handlers import SinkHandler
from .levels import CRITICAL, DEBUG, ERROR, INFO, NOTSET, TRACE, WARNING
from .manager import BoundLogger, LogManager, Processor
from .processors import RateLimit, execution_context
from .queue import QueueHandler, QueueStats
from .sinks import FileSink, StandardStreamSink, StreamSink, TextSink

__all__: list[str] = [
    "CRITICAL",
    "DEBUG",
    "ERROR",
    "INFO",
    "NOTSET",
    "TRACE",
    "WARNING",
    "BoundLogger",
    "FileSink",
    "JsonFormatter",
    "LogManager",
    "Processor",
    "QueueHandler",
    "QueueStats",
    "RateLimit",
    "SinkHandler",
    "StandardStreamSink",
    "StreamSink",
    "TextSink",
    "execution_context",
    "redact",
]
