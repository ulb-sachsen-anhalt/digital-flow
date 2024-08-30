"""common constants"""

import logging
import sys

XMLNS = {
    'alto': 'http://www.loc.gov/standards/alto/ns-v4#',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dv': 'http://dfg-viewer.de/',
    'epicur': 'urn:nbn:de:1111-2004033116',
    'marcxml': 'http://www.loc.gov/MARC21/slim',
    'goobi': 'http://meta.goobi.org/v1.5.1/',
    'mets': 'http://www.loc.gov/METS/',
    'mix': 'http://www.loc.gov/mix/v20',
    'mods': 'http://www.loc.gov/mods/v3',
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
    'ulb': 'https://bibliothek.uni-halle.de',
    'vl': 'http://visuallibrary.net/vl',
    'vlz': 'http://visuallibrary.net/vlz/1.0/',
    'xlink': 'http://www.w3.org/1999/xlink',
    'zvdd': 'http://zvdd.gdz-cms.de/',
}


UNSET_LABEL = 'n.a.'


class FallbackLogger:
    """Different way to inject logging facilities"""

    def __init__(self, some_logger=None):
        self._logger: logging.Logger = some_logger

    def log(self, message: str, *args, level = logging.INFO):
        """Encapsulate Loggin"""
        if self._logger:
            self._logger.log(level, message, *args)
        else:
            message = message.replace('%s','{}')
            if args is not None and len(args) > 0:
                message = message.format(*args)
            if level >= logging.ERROR:
                print(message, file=sys.stderr)
            else:
                print(message)
