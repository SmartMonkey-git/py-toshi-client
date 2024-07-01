class ToshiError(Exception):
    pass


class ToshiClientError(ToshiError):
    pass


class ToshiIndexError(ToshiError):
    pass


class ToshiDocumentError(ToshiError):
    pass


class ToshiFlushError(ToshiError):
    pass
