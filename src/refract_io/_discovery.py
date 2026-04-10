from __future__ import annotations

import logging
import socket
import threading
import time

from zeroconf import ServiceBrowser, ServiceInfo, ServiceStateChange, Zeroconf

logger = logging.getLogger(__name__)

SERVICE_TYPE = "_refract._tcp.local."

# Bonjour TXT: state=waiting|active; transport=raw-tcp (ignored) or refract-stream
TXT_STATE_KEY = b"state"
TXT_TRANSPORT_KEY = b"transport"
REFRACT_STREAM_TRANSPORT = "refract-stream"


def _txt_stream_state(
    properties: dict[bytes, bytes | None] | None,
) -> str | None:
    if not properties:
        return None
    raw = properties.get(TXT_STATE_KEY)
    if raw is None:
        return None
    return raw.decode("utf-8", errors="replace").lower().strip()


def _txt_transport(
    properties: dict[bytes, bytes | None] | None,
) -> str | None:
    if not properties:
        return None
    raw = properties.get(TXT_TRANSPORT_KEY)
    if raw is None:
        return None
    return raw.decode("utf-8", errors="replace").lower().strip()


def _state_rank(state: str | None) -> int:
    """Lower is preferred; ``active`` is last. Missing/unknown = waiting tier."""
    if state == "active":
        return 1
    return 0


def _host_port_from_info(info: ServiceInfo) -> tuple[str, int] | None:
    addresses = info.parsed_addresses()
    if not addresses or info.port is None:
        return None
    port = int(info.port)
    host = addresses[0]
    for addr in addresses:
        try:
            packed = socket.inet_aton(addr)
            if packed[0:2] != b"\xa9\xfe":  # not 169.254.x.x
                host = addr
                break
        except OSError:
            continue
    return (host, port)


def discover_refract(timeout: float = 3.0) -> tuple[str, int] | None:
    """Browse for a Refract Stream (gRPC) service advertised via Bonjour.

    Listens for ``_refract._tcp`` with TXT ``transport=refract-stream``. For
    *timeout* seconds it collects matches, then picks one by TXT ``state``:

    * ``state=waiting`` (or no ``state`` key) — preferred.
    * ``state=active`` — used only if no waiting instance exists.

    Rows with ``transport=raw-tcp`` (plain TCP mode in the app) are ignored.

    Returns ``(host, port)`` or ``None`` if nothing suitable is found.
    """
    by_name: dict[str, ServiceInfo] = {}
    order: list[str] = []
    lock = threading.Lock()

    def remember(info: ServiceInfo | None, name: str) -> None:
        if info is None:
            return
        if _txt_transport(info.properties) != REFRACT_STREAM_TRANSPORT:
            return
        hp = _host_port_from_info(info)
        if hp is None:
            return
        with lock:
            if name not in by_name:
                order.append(name)
            by_name[name] = info

    def on_state_change(
        zeroconf: Zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ) -> None:
        if state_change is ServiceStateChange.Removed:
            with lock:
                by_name.pop(name, None)
                if name in order:
                    order.remove(name)
            return
        if state_change not in (
            ServiceStateChange.Added,
            ServiceStateChange.Updated,
        ):
            return
        info = zeroconf.get_service_info(service_type, name, timeout=1500)
        remember(info, name)

    zc = Zeroconf()
    try:
        ServiceBrowser(zc, SERVICE_TYPE, handlers=[on_state_change])
        time.sleep(timeout)
    finally:
        zc.close()

    with lock:
        entries = [(name, by_name[name]) for name in order if name in by_name]

    if not entries:
        return None

    def sort_key(item: tuple[str, ServiceInfo]) -> tuple[int, int]:
        name, info = item
        rank = _state_rank(_txt_stream_state(info.properties))
        idx = order.index(name)
        return (rank, idx)

    entries.sort(key=sort_key)
    chosen_name, chosen = entries[0]
    hp = _host_port_from_info(chosen)
    if hp is None:
        return None
    host, port = hp
    st = _txt_stream_state(chosen.properties)
    tr = _txt_transport(chosen.properties)
    logger.debug(
        "Discovered Refract at %s:%d (name=%r state=%r transport=%r)",
        host,
        port,
        chosen_name,
        st,
        tr,
    )
    return (host, port)
