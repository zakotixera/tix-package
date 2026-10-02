"""
Process-local text destinations, independent of records and formatting.
"""

from __future__ import annotations

import sys
import threading
from _thread import RLock
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal, Protocol, TextIO

from ._runtime import ProcessGuard


class TextSink(
    Protocol,
):
    """
    Accept complete text writes and expose explicit flush and close operations.
    """

    def write(
        self: TextSink,
        text: str,
    ) -> None:
        """
        Write text or raise on failure.

        Args:
            text: Fully rendered text, including any record terminator.
        """
        ...

    def flush(
        self: TextSink,
    ) -> None:
        """
        Flush previously written text to the underlying destination.
        """
        ...

    def close(
        self: TextSink,
    ) -> None:
        """
        Flush and release owned resources; repeated calls are harmless.
        """
        ...


class _TextOutput(
    ABC,
):
    """
    Serialize text I/O and reject writes after closure or process inheritance.
    """

    def __init__(
        self: _TextOutput,
        owned: bool,
    ) -> None:
        """
        Initialize lifecycle state.

        Args:
            owned: Whether closing the sink also closes its stream.
        """
        self._guard: ProcessGuard = ProcessGuard()
        self._lock: RLock = threading.RLock()
        self._owned: bool = owned
        self._closed: bool = False

    @abstractmethod
    def _get_stream(
        self: _TextOutput,
        opening: bool,
    ) -> TextIO | None:
        """
        Resolve the stream while holding the I/O lock.

        Args:
            opening: Whether a lazy destination should be opened for writing.
        """
        ...

    def write(
        self: _TextOutput,
        text: str,
    ) -> None:
        """
        Serialize one complete write, reporting partial writes as failures.

        Args:
            text: Fully rendered text to write without adding a terminator.
        """
        self._guard.check()
        with self._lock:
            if self._closed:
                raise RuntimeError("Text sink is closed")
            stream: TextIO | None = self._get_stream(True)
            assert stream is not None
            written: int = stream.write(text)
            if written != len(text):
                raise OSError("Text stream did not accept the complete write")

    def flush(
        self: _TextOutput,
    ) -> None:
        """
        Flush an open destination without opening an unused file.
        """
        if not self._guard.owned:
            return
        with self._lock:
            if not self._closed:
                stream: TextIO | None = self._get_stream(False)
                if stream is not None:
                    stream.flush()

    def close(
        self: _TextOutput,
    ) -> None:
        """
        Flush once and close owned streams even when flushing raises.
        """
        if not self._guard.owned:
            return
        with self._lock:
            if self._closed:
                return
            self._closed = True
            stream: TextIO | None = self._get_stream(False)
            if stream is not None:
                try:
                    stream.flush()
                finally:
                    if self._owned:
                        stream.close()


class StreamSink(
    _TextOutput,
):
    """
    Write to an injected text stream, borrowed unless explicitly owned.
    """

    def __init__(
        self: StreamSink,
        stream: TextIO,
        *,
        close_stream: bool = False,
    ) -> None:
        """
        Configure an injected destination.

        Args:
            stream: Writable text stream, such as StringIO or an open file.
            close_stream: Whether closing this sink also closes the stream.
        """
        super().__init__(close_stream)
        self._stream: TextIO = stream

    def _get_stream(
        self: StreamSink,
        opening: bool,
    ) -> TextIO:
        """
        Return the injected stream.

        Args:
            opening: Unused because the injected stream is already open.
        """
        return self._stream


class StandardStreamSink(
    _TextOutput,
):
    """
    Resolve stdout or stderr on each operation and never close either stream.
    """

    def __init__(
        self: StandardStreamSink,
        stream: Literal["stdout", "stderr"] = "stderr",
    ) -> None:
        """
        Select a standard stream, respecting subsequent redirection.

        Args:
            stream: Standard stream name resolved at I/O time.
        """
        if stream not in {"stdout", "stderr"}:
            raise ValueError("stream must be stdout or stderr")
        super().__init__(False)
        self._name: Literal["stdout", "stderr"] = stream

    def _get_stream(
        self: StandardStreamSink,
        opening: bool,
    ) -> TextIO:
        """
        Resolve the current process standard stream.

        Args:
            opening: Whether the caller intends to write to the stream.
        """
        stream: TextIO | None = sys.stdout if self._name == "stdout" else sys.stderr
        if stream is None:
            raise RuntimeError(f"sys.{self._name} is unavailable")
        return stream


class FileSink(
    _TextOutput,
):
    """
    Own a buffered text file, opened lazily and never reopened after closure.
    """

    def __init__(
        self: FileSink,
        path: str | Path,
        *,
        mode: Literal["a", "w", "x"] = "a",
        encoding: str = "utf-8",
        errors: str = "backslashreplace",
        buffering: int = -1,
        create_parents: bool = False,
    ) -> None:
        """
        Store file configuration without touching the filesystem.

        Args:
            path: Destination resolved relative to the construction directory.
            mode: Append, truncate on first write, or create exclusively.
            encoding: Text encoding used by the owned file.
            errors: Encoding error policy.
            buffering: Text I/O buffer size; -1 selects the platform default.
            create_parents: Whether the first write creates missing directories.
        """
        if mode not in {"a", "w", "x"}:
            raise ValueError("mode must be a, w, or x")
        if not isinstance(buffering, int) or buffering == 0 or buffering < -1:
            raise ValueError("buffering must be -1 or a positive integer")
        super().__init__(True)
        self._path: Path = Path(path).absolute()
        self._mode: Literal["a", "w", "x"] = mode
        self._encoding: str = encoding
        self._errors: str = errors
        self._buffering: int = buffering
        self._create_parents: bool = create_parents
        self._stream: TextIO | None = None

    def _get_stream(
        self: FileSink,
        opening: bool,
    ) -> TextIO | None:
        """
        Open the file on its first write, preserving existing open state.

        Args:
            opening: Whether an unopened file should be created or opened.
        """
        if self._stream is None and opening:
            if self._create_parents:
                self._path.parent.mkdir(parents=True, exist_ok=True)
            self._stream = self._path.open(
                self._mode,
                buffering=self._buffering,
                encoding=self._encoding,
                errors=self._errors,
                newline="\n",
            )
        return self._stream
