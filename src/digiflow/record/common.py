"""Common record attributes"""

import ast
import json
import time
import typing

import digiflow as df


COMMENT_MARK = '#'

STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

RECORD_STATE_MASK_FRAME = 'other_load'

UNSET_LABEL = 'n.a.'

FIELD_IDENTIFIER = 'IDENTIFIER'
FIELD_URN = 'URN'
FIELD_SYSTEM_HANDLE = 'HANDLE'
FIELD_SPEC = 'SETSPEC'
FIELD_DATESTAMP = 'CREATED'
FIELD_INFO = 'INFO'
FIELD_STATE = 'STATE'
FIELD_STATETIME = 'STATE_TIME'

LEGACY_HEADER = [FIELD_IDENTIFIER, FIELD_SPEC, FIELD_DATESTAMP,
                 FIELD_INFO, FIELD_STATE, FIELD_STATETIME]
RECORD_HEADER = [FIELD_IDENTIFIER, FIELD_INFO,
                 FIELD_STATE, FIELD_STATETIME]

DEFAULT_MAPPINGS = {
    'identifier': FIELD_IDENTIFIER,
    'ext_urn': FIELD_URN,
    'system_handle': FIELD_SYSTEM_HANDLE,
    'setspec': FIELD_SPEC,
    'created_time': FIELD_DATESTAMP,
    'info': FIELD_INFO,
    'state': FIELD_STATE,
    'state_time': FIELD_STATETIME,
}


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
    Record based on valid OAI-URN-Identifier with optional setspec data
    based on http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm
    transported via OAI-PMH API or delivered by RecordService

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
        self.ext_urn = UNSET_LABEL
        self.system_handle = UNSET_LABEL
        self.set = UNSET_LABEL
        self.created_time = UNSET_LABEL
        self._info = UNSET_LABEL
        self._state = UNSET_LABEL
        self.state_time = UNSET_LABEL

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
        """Get record identifier"""
        return self.__urn

    def __str__(self) -> str:
        the_str = f"{self.__urn}"
        if self.set != df.UNSET_LABEL:
            the_str = f"{the_str}\t{self.set}"
        if self.created_time != df.UNSET_LABEL:
            the_str = f"{the_str}\t{self.created_time}"
        if self._info != df.UNSET_LABEL:
            the_str = f"{the_str}\t{self._info}"
        return f"{the_str}\t{self._state}\t{self.state_time}"

    @staticmethod
    def parse(input_data):
        """De-serialize record from different input forms"""
        record = Record(UNSET_LABEL)
        if isinstance(input_data, dict):
            record = row_to_record(input_data)
        return record

    def dict(self, dict_map=None) -> typing.Dict:
        """Serialize Record into Python dict
        as input for JSON load.
        Please note: Tries to dump deep structures
            and yields exception if record unlikely
            to be JSON serializable
        """
        as_dict = {}
        if dict_map is None:
            dict_map = DEFAULT_MAPPINGS
        for label, field in dict_map.items():
            if hasattr(self, label):
                as_dict[field] = getattr(self, label)
        try:
            json.dumps(as_dict)
        except TypeError as struct_err:
            err_msg = f"{struct_err.args[0]} => {self.info}"
            raise RecordDataException(err_msg) from struct_err
        return as_dict

    @property
    def state(self):
        """Get state"""
        return self._state

    @state.setter
    def state(self, state_label):
        """Set new state and update statetime"""

        self._state = state_label
        right_now = time.strftime(STATETIME_FORMAT)
        self.state_time = right_now

    @property
    def info(self):
        """Get Record Information"""
        return self._info

    @info.setter
    def info(self, any_value):
        """Update existing Information lazy.
        Assume info consists of at least
        a single dict or several dicts,
        in which case only the last dict
        will be updated"""

        try:
            if any_value == UNSET_LABEL:
                any_value = {}
            if self._info == UNSET_LABEL:
                self._info = {}
            if isinstance(any_value, str):
                any_value = ast.literal_eval(any_value)
            elif isinstance(self._info, str):
                self._info = ast.literal_eval(self._info)
            if isinstance(self._info, dict):
                self._info.update(any_value)
            elif isinstance(self._info, tuple):
                self._info[-1].update(any_value)
        except (AttributeError, SyntaxError, ValueError):
            self._info = any_value


def row_to_record(row: typing.Dict):
    """Serialize data row to Record with all
    set attributes filled and mark invalid
    if basic attributes unset
    """

    if FIELD_IDENTIFIER not in row:
        raise RecordDataException(f"Missing {FIELD_IDENTIFIER} in {row}")
    record = Record(row[FIELD_IDENTIFIER])
    if FIELD_URN in row and str(row[FIELD_URN]).strip():
        record.ext_urn = row[FIELD_URN]
    if FIELD_SYSTEM_HANDLE in row and str(row[FIELD_SYSTEM_HANDLE]).strip():
        record.system_handle = row[FIELD_SYSTEM_HANDLE]
    if FIELD_SPEC in row and str(row[FIELD_SPEC]).strip():
        record.set = str(row[FIELD_SPEC]).strip()
    if FIELD_DATESTAMP in row and str(row[FIELD_DATESTAMP]).strip():
        record.created_time = str(row[FIELD_DATESTAMP]).strip()
    if FIELD_INFO in row and str(FIELD_INFO).strip():
        record.info = str(row[FIELD_INFO]).strip()
    if FIELD_STATE not in row:
        record.state = UNSET_LABEL
    else:
        record.state = row[FIELD_STATE]
    if FIELD_STATETIME in row:
        record.state_time = row[FIELD_STATETIME]
    return record
