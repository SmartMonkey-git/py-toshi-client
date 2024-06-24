class ToshiError(Exception):
    pass


class ToshiIndexError(ToshiError):
    pass


class ToshiDocumentError(ToshiError):
    pass


class ToshiFlushError(ToshiError):
    pass
