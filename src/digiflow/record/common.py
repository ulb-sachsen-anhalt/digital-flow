"""Common record attributes"""

import typing

import digiflow as df


COMMENT_MARK = '#'

STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

RECORD_STATE_MASK_FRAME = 'other_load'

UNSET_LABEL = 'n.a.'

FIELD_IDENTIFIER = 'IDENTIFIER'
FIELD_SPEC = 'SETSPEC'
FIELD_DATESTAMP = 'CREATED'
FIELD_INFO = 'INFO'
FIELD_STATE = 'STATE'
FIELD_STATETIME = 'STATE_TIME'

LEGACY_HEADER = [FIELD_IDENTIFIER, FIELD_SPEC, FIELD_DATESTAMP,
                 FIELD_INFO, FIELD_STATE, FIELD_STATETIME]
RECORD_HEADER = [FIELD_IDENTIFIER, FIELD_INFO,
                 FIELD_STATE, FIELD_STATETIME]


class RecordDataException(Exception):
    """Mark inconsistent record data,
    i.e. Record laking the three basic
    attributes
    * identifier
    * state
    * state_time
    """


class Record:
    """
    OAIRecord based on valid URN-Identifier with optional set specification data
    based on http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm
    and commonly transported via OAI-PMH API

    Examples:

    * oai:digital.bibliothek.uni-halle.de/hd:10595
    * oai:digitale.bibliothek.uni-halle.de/vd18:9427342
    * oai:digitale.bibliothek.uni-halle.de/zd:9633001
    * oai:opendata.uni-halle.de:1981185920/34265
    * oai:menadoc.bibliothek.uni-halle.de/menadoc:20586
    * oai:dev.opendata.uni-halle.de:123456789/27949

    """

    def __init__(self, urn):
        self.__urn = urn
        self.__local_ident = None
        self.set = UNSET_LABEL
        self.date_stamp = UNSET_LABEL
        self.info = UNSET_LABEL
        self.state = UNSET_LABEL
        self.state_datetime = UNSET_LABEL

    @property
    def local_identifier(self):
        """
        source identifier for local usage
        as sub-directory in local storages
        * remove any related urn prefix (like oai)
        * replace possible '/' in handle urns
        """
        if not self.__local_ident:
            _local_ident = self.__urn
            if ':' in _local_ident:
                _splits = self.__urn.split(':')
                _local_ident = _splits[-1]
            if '/' in _local_ident:
                _local_ident = _local_ident.replace('/', '_')
            self.__local_ident = _local_ident
        return self.__local_ident

    @property
    def identifier(self):
        return self.__urn

    def __str__(self) -> str:
        the_str = f"{self.__urn}"
        if self.set != df.UNSET_LABEL:
            the_str = f"{the_str}\t{self.set}"
        if self.date_stamp != df.UNSET_LABEL:
            the_str = f"{the_str}\t{self.date_stamp}"
        if self.info != df.UNSET_LABEL:
            the_str = f"{the_str}\t{self.info}"
        return f"{the_str}\n{self.state}\t{self.state_datetime}"


def row_to_record(row: typing.Dict):
    """Serialize data row to Record with all
    set attributes filled and mark invalid
    if basic attributes unset
    """

    if FIELD_IDENTIFIER not in row:
        raise RecordDataException(f"Missing {FIELD_IDENTIFIER} in {row}")
    record = Record(row[FIELD_IDENTIFIER])
    if FIELD_SPEC in row and str(row[FIELD_SPEC]).strip():
        record.set = str(row[FIELD_SPEC]).strip()
    if FIELD_DATESTAMP in row and str(row[FIELD_DATESTAMP]).strip():
        record.date_stamp = str(row[FIELD_DATESTAMP]).strip()
    if FIELD_INFO in row and str(FIELD_INFO).strip():
        record.info = str(row[FIELD_INFO]).strip()
    if FIELD_STATE not in row:
        raise RecordDataException(f"Missing {FIELD_STATE} in {row}")
    record.state = row[FIELD_STATE]
    if FIELD_STATETIME not in row:
        raise RecordDataException(f"Missing {FIELD_STATETIME} in {row}")
    record.state_datetime = row[FIELD_STATETIME]
    return record
