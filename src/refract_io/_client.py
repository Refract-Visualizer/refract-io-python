from __future__ import annotations

import logging
import queue
import struct
import threading
from collections.abc import Generator, Sequence
from types import TracebackType
from typing import Any

import grpc

from refract_io._discovery import discover_refract
from refract_io._proto import kvstream_pb2 as _pb2
from refract_io._proto import kvstream_pb2_grpc as _pb2_grpc
from refract_io._types import DTYPE_INFO, ValueType

# Re-bind as Any so mypy doesn't complain about dynamic proto attributes
kvstream_pb2: Any = _pb2
kvstream_pb2_grpc: Any = _pb2_grpc

logger = logging.getLogger(__name__)

_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 50051

# Sentinel used to signal the background thread to stop.
_STOP = object()

_GRPC_PEER_GONE: frozenset[grpc.StatusCode] = frozenset(
    {
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.ABORTED,
    }
)


class RefractStream:
    """Stream tabular data over gRPC to the Refract app.

    ``host`` and ``port`` are optional.  When both are ``None`` the client
    attempts Bonjour autodiscovery (``_refract._tcp`` with
    ``transport=refract-stream``; prefers TXT ``state=waiting`` over
    ``state=active``) and falls back to
    ``localhost:50051``.
    ``port`` accepts either an ``int`` or a numeric ``str``.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | str | None = None,
    ) -> None:
        resolved_host, resolved_port = self._resolve_address(host, port)
        self._host: str = resolved_host
        self._port: int = resolved_port

        self._channel: grpc.Channel | None = None
        self._stub: kvstream_pb2_grpc.KVStreamStub | None = None
        self._queue: queue.Queue[object] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._running: bool = False

        self._lock = threading.Lock()
        # table_id -> list of (struct_fmt, byte_size) per column
        self._tables: dict[int, list[tuple[str, int]]] = {}
        self._pending_registrations: list[kvstream_pb2.StreamMessage] = []

    # ------------------------------------------------------------------
    # Address resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_address(
        host: str | None,
        port: int | str | None,
    ) -> tuple[str, int]:
        if host is not None or port is not None:
            return (
                host if host is not None else _DEFAULT_HOST,
                int(port) if port is not None else _DEFAULT_PORT,
            )
        # Autodiscover
        result = discover_refract()
        if result is not None:
            logger.info("Autodiscovered Refract at %s:%d", *result)
            return result
        logger.info(
            "No Refract service found via Bonjour, " "falling back to %s:%d",
            _DEFAULT_HOST,
            _DEFAULT_PORT,
        )
        return (_DEFAULT_HOST, _DEFAULT_PORT)

    # ------------------------------------------------------------------
    # Table registration
    # ------------------------------------------------------------------

    def create_table(
        self,
        id: int,
        name: str,
        columns: dict[str, ValueType],
    ) -> None:
        """Register a table schema.

        ``columns`` maps column names to :class:`ValueType` constants
        (e.g. ``{'timestamp': refract_io.float64, ...}``).
        """
        col_defs: list[kvstream_pb2.ColumnDef] = []
        fmt_parts: list[tuple[str, int]] = []
        for col_name, vtype in columns.items():
            vtype = ValueType(vtype)  # validate
            fmt, size = DTYPE_INFO[vtype]
            col_defs.append(
                kvstream_pb2.ColumnDef(name=col_name, value_type=int(vtype))
            )
            fmt_parts.append((fmt, size))

        with self._lock:
            self._tables[id] = fmt_parts

        msg = kvstream_pb2.StreamMessage(
            register_table=kvstream_pb2.RegisterTable(
                table_id=id,
                name=name,
                columns=col_defs,
            )
        )
        if self._running:
            self._queue.put(msg)
        else:
            self._pending_registrations.append(msg)

    # ------------------------------------------------------------------
    # Sending rows
    # ------------------------------------------------------------------

    def send_row(self, table_id: int, values: Sequence[int | float]) -> None:
        """Send a single row for *table_id*.

        Values are packed in registration order using the column dtypes.
        Auto-starts the connection if not already running.
        """
        self._ensure_started()

        with self._lock:
            fmt_parts = self._tables.get(table_id)
        if fmt_parts is None:
            raise ValueError(f"Table {table_id} not registered")
        if len(values) != len(fmt_parts):
            raise ValueError(f"Expected {len(fmt_parts)} values, got {len(values)}")

        packed = b"".join(
            struct.pack(fmt, val)
            for val, (fmt, _) in zip(values, fmt_parts, strict=True)
        )
        msg = kvstream_pb2.StreamMessage(
            table_row=kvstream_pb2.TableRow(
                table_id=table_id,
                values=packed,
            )
        )
        self._queue.put(msg)

    def send_rows(
        self,
        table_id: int,
        rows: Sequence[Sequence[int | float]],
    ) -> None:
        """Send multiple rows for *table_id* in one call."""
        self._ensure_started()

        with self._lock:
            fmt_parts = self._tables.get(table_id)
        if fmt_parts is None:
            raise ValueError(f"Table {table_id} not registered")

        expected = len(fmt_parts)
        for row in rows:
            if len(row) != expected:
                raise ValueError(f"Expected {expected} values per row, got {len(row)}")
            packed = b"".join(
                struct.pack(fmt, val)
                for val, (fmt, _) in zip(row, fmt_parts, strict=True)
            )
            msg = kvstream_pb2.StreamMessage(
                table_row=kvstream_pb2.TableRow(
                    table_id=table_id,
                    values=packed,
                )
            )
            self._queue.put(msg)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Open the gRPC channel and start the background streaming thread."""
        if self._running:
            return
        self._running = True
        self._channel = grpc.insecure_channel(f"{self._host}:{self._port}")
        self._stub = kvstream_pb2_grpc.KVStreamStub(self._channel)
        self._thread = threading.Thread(
            target=self._stream_loop, daemon=True, name="refract-stream"
        )
        self._thread.start()

    def close(self) -> None:
        """Stop the background thread and close the gRPC channel."""
        if not self._running:
            return
        self._running = False
        try:
            self._queue.put_nowait(_STOP)
        except queue.Full:
            pass
        thread = self._thread
        ch = self._channel
        self._channel = None
        if ch is not None:
            try:
                ch.close()
            except Exception:
                logger.debug("Error closing gRPC channel", exc_info=True)
        if thread is not None:
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning("Stream thread did not exit within join timeout")
            self._thread = None

    stop = close

    def _ensure_started(self) -> None:
        if not self._running:
            self.start()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> RefractStream:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Background streaming
    # ------------------------------------------------------------------

    def _message_generator(
        self,
    ) -> Generator[kvstream_pb2.StreamMessage, None, None]:
        # Send pending registrations first
        yield from self._pending_registrations
        self._pending_registrations.clear()

        # Stream from queue; exit when idle after *close* even if gRPC is slow.
        while True:
            try:
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                if not self._running:
                    break
                continue
            if item is _STOP:
                break
            assert isinstance(item, kvstream_pb2.StreamMessage)  # noqa: S101
            yield item

    def _stream_loop(self) -> None:
        try:
            assert self._stub is not None  # noqa: S101
            response = self._stub.Stream(self._message_generator())
            if not response.ok:
                logger.error("Server error: %s", response.error)
        except grpc.RpcError as exc:
            if not self._running:
                return
            if exc.code() in _GRPC_PEER_GONE:
                logger.debug(
                    "gRPC stream ended (%s): %s",
                    exc.code().name,
                    exc.details() or exc,
                )
                return
            logger.error("gRPC error: %s", exc)
        except Exception:
            if self._running:
                logger.exception("Unexpected error in stream loop")
