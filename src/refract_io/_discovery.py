from __future__ import annotations

import logging
import socket
import threading

from zeroconf import ServiceBrowser, ServiceStateChange, Zeroconf

logger = logging.getLogger(__name__)

SERVICE_TYPE = "_refract-stream._tcp.local."


def discover_refract(timeout: float = 3.0) -> tuple[str, int] | None:
    """Browse for a Refract Stream service advertised via Bonjour.

    Returns ``(host, port)`` of the first instance found, or ``None``
    if no service is discovered within *timeout* seconds.
    """
    result: tuple[str, int] | None = None
    found = threading.Event()

    def on_state_change(
        zeroconf: Zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ) -> None:
        nonlocal result
        if state_change is not ServiceStateChange.Added:
            return
        info = zeroconf.get_service_info(service_type, name)
        if info is None:
            return
        addresses = info.parsed_addresses()
        if not addresses or info.port is None:
            return
        port = info.port
        # Prefer non-link-local IPv4 addresses
        host = addresses[0]
        for addr in addresses:
            try:
                packed = socket.inet_aton(addr)
                if packed[0:2] != b"\xa9\xfe":  # not 169.254.x.x
                    host = addr
                    break
            except OSError:
                continue
        result = (host, port)
        logger.debug("Discovered Refract at %s:%d", host, port)
        found.set()

    zc = Zeroconf()
    try:
        ServiceBrowser(zc, SERVICE_TYPE, handlers=[on_state_change])
        found.wait(timeout=timeout)
    finally:
        zc.close()

    return result
