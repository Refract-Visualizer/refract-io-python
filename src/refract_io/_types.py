from __future__ import annotations

from enum import IntEnum


class ValueType(IntEnum):
    """Column data types matching the kvstream protobuf ValueType enum."""

    FLOAT32 = 0
    FLOAT64 = 1
    INT8 = 2
    INT16 = 3
    INT32 = 4
    INT64 = 5
    UINT8 = 6
    UINT16 = 7
    UINT32 = 8
    UINT64 = 9


# Internal mapping: ValueType -> (struct format, byte size)
DTYPE_INFO: dict[ValueType, tuple[str, int]] = {
    ValueType.FLOAT32: ("<f", 4),
    ValueType.FLOAT64: ("<d", 8),
    ValueType.INT8: ("<b", 1),
    ValueType.INT16: ("<h", 2),
    ValueType.INT32: ("<i", 4),
    ValueType.INT64: ("<q", 8),
    ValueType.UINT8: ("<B", 1),
    ValueType.UINT16: ("<H", 2),
    ValueType.UINT32: ("<I", 4),
    ValueType.UINT64: ("<Q", 8),
}
