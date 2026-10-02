"""
Supported severities, compatible with standard logging and extended with TRACE.
"""

import logging
from typing import Final

NOTSET: Final[int] = logging.NOTSET
TRACE: Final[int] = 5
DEBUG: Final[int] = logging.DEBUG
INFO: Final[int] = logging.INFO
WARNING: Final[int] = logging.WARNING
ERROR: Final[int] = logging.ERROR
CRITICAL: Final[int] = logging.CRITICAL
