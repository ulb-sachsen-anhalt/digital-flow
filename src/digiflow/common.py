"""common constants"""

import functools
import logging
import sys
import warnings

XMLNS = {
    "alto": "http://www.loc.gov/standards/alto/ns-v4#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dv": "http://dfg-viewer.de/",
    "epicur": "urn:nbn:de:1111-2004033116",
    "marcxml": "http://www.loc.gov/MARC21/slim",
    "goobi": "http://meta.goobi.org/v1.5.1/",
    "mets": "http://www.loc.gov/METS/",
    "mix": "http://www.loc.gov/mix/v20",
    "mods": "http://www.loc.gov/mods/v3",
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "ulb": "https://bibliothek.uni-halle.de",
    "vl": "http://visuallibrary.net/vl",
    "vlz": "http://visuallibrary.net/vlz/1.0/",
    "xlink": "http://www.w3.org/1999/xlink",
    "zvdd": "http://zvdd.gdz-cms.de/",
}


UNSET_LABEL = "n.a."


def deprecated(reason):
    """Mark a function as deprecated"""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__} is deprecated: {reason}",
                category=DeprecationWarning,
                stacklevel=2,
            )
            return func(*args, **kwargs)

        return wrapper

    return decorator


class FallbackLogger:
    """Different way to inject logging facilities"""

    def __init__(self, some_logger=None):
        if some_logger is None:
            some_logger = logging.getLogger(__name__)
        self._logger: logging.Logger = some_logger

    def log(self, message: str, *args, level=logging.INFO):
        """Encapsulate Loggin"""
        if self._logger:
            self._logger.log(level, message, *args)
        else:
            message = message.replace("%s", "{}")
            if args is not None and len(args) > 0:
                message = message.format(*args)
            if level >= logging.ERROR:
                print(message, file=sys.stderr)
            else:
                print(message)


class METSFile:
    """Represents fileGrp/file entry with file locations
    and unspecified optional attributes"""

    def __init__(
        self, use: str, file_id: str, mime_type: str, url: str, **kwargs
    ) -> None:
        self.use = use
        self.file_id = file_id
        self.mimetype = mime_type
        self.url = url
        for key, value in kwargs.items():
            setattr(self, key.lower(), value)
