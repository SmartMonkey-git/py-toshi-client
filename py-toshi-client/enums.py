from enum import Enum


class IndexTypes(str, Enum):
    TEXT = "text"
    U64 = "u64"
    I64 = "i64"
    F64 = "f64"
    BOOL = "bool"
    DATE = "date"  # TODO: Not implemented, yet
    JSON = "json"  # TODO: Not implemented, yet
    BYTES = "bytes"  # TODO: Not implemented, yet
    IP = "6u8"  # TODO: Not implemented, yet
