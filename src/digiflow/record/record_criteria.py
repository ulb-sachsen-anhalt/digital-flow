"""API to filter Records from data set"""

import abc
import collections
import time

import digiflow.record as df_r


class Criteria:
    """Criteria to select Records"""

    @abc.abstractmethod
    def matched(self, _: collections.OrderedDict) -> bool:
        """Determine, whether given OAI-Record matched criteria"""


class Identifier(Criteria):
    """Expect an OAI-URN as identifier,
    therefore assume prefix 'oai:' 
    otherwise use last ':' segment
    """

    def __init__(self, ident):
        self.identifier = ident

    def matched(self, record: collections.OrderedDict) -> bool:
        rec_id = record[df_r.FIELD_IDENTIFIER]
        # maybe must deal shortened identifier (like legacy id)
        if 'oai' not in self.identifier or ':' not in self.identifier:
            rec_id = rec_id.split(':')[-1]
        return self.identifier == rec_id


class State(Criteria):
    """Select by actual STATE"""

    def __init__(self, state):
        self.state = state

    def matched(self, record: collections.OrderedDict) -> bool:
        record_state = record[df_r.FIELD_STATE]
        return self.state == record_state


class Datetime(Criteria):
    """
    Use datetime data to match OAIRecords.\n
    Match field 'STATE_TIME' (or 'CREATED')\n
    Default datetime pattern:
        'dt_pattern' = '%Y-%m-%d_%H:%M:%S' (fits 'STATE_TIME'),\n
        can be overwritten ('CREATED' uses commonly: '%Y-%m-%dT%H:%M:%SZ').
    """

    def __init__(self, **kwargs):
        self.dt_pattern = df_r.STATETIME_FORMAT
        self.field = df_r.FIELD_STATETIME
        self.dt_from = None
        self.dt_to = None
        # *ORDER MATTERS*
        if 'dt_field' in kwargs:
            self.field = kwargs['dt_field']
        if 'dt_format' in kwargs:
            self.dt_pattern = kwargs['dt_format']
        if 'dt_from' in kwargs:
            self.dt_from = time.strptime(kwargs['dt_from'], self.dt_pattern)
        if 'dt_to' in kwargs:
            self.dt_to = time.strptime(kwargs['dt_to'], self.dt_pattern)

    def matched(self, record: collections.OrderedDict) -> bool:
        if self.field not in record:
            raise RuntimeError(f"{self.field} not in {record}")
        record_state_ts = record[self.field]
        if record_state_ts == df_r.UNSET_LABEL:
            return False
        record_ts = time.strptime(record_state_ts, self.dt_pattern)
        if self.dt_from and not self.dt_to:
            return record_ts >= self.dt_from
        elif self.dt_from and self.dt_to:
            return (record_ts >= self.dt_from) and (record_ts < self.dt_to)
        elif not self.dt_from and self.dt_to:
            return record_ts < self.dt_to
        return False


class Text(Criteria):
    """Search for complete enclosing
    string, not via pattern"""

    def __init__(self, text, field=df_r.FIELD_INFO) -> None:
        super().__init__()
        self.text = text
        self.field = field

    def matched(self, record: collections.OrderedDict) -> bool:
        return self.text in record[self.field]
