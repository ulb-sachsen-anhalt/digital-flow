# -*- coding: utf-8 -*-

import csv
import os
import shutil
import time
import smtplib
from email.mime.text import (
    MIMEText
)

from collections import (
    OrderedDict
)
from pathlib import (
    Path
)

import requests
from lxml import etree as ET

from .digiflow_metadata import (
    XMLNS,
    write_xml_file,
    MetsReader
)


####
#
# OAIRecord Specification (based on Migration)
#
F_IDENTIFIER = 'IDENTIFIER'
F_SPEC = 'SETSPEC'
F_DATESTAMP = 'CREATED'
F_STATE_INFO = 'INFO'
F_STATE = 'STATE'
F_STATE_TS = 'STATE_TIME'
HEADER_MIGRATION = [F_IDENTIFIER, F_SPEC, F_DATESTAMP,
                    F_STATE_INFO, F_STATE, F_STATE_TS]
COMMENT_MARK = '#'
UNSET = 'n.a.'
RECORD_STATE_UNSET = UNSET
RECORD_STATE_MASK_FRAME = 'other_load'
SETSPEC_SPLITTER = '##'
STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'
STATETIME_FORMAT_ALT = '%Y-%m-%dT%H:%M:%SZ'


def post_oai_extract_metsdata(xml_tree):
    """Extract METS as new root from OAI envelope"""

    namespace = xml_tree.xpath('namespace-uri(.)')
    if namespace == 'http://www.loc.gov/METS/':
        return xml_tree

    if namespace == 'http://www.openarchives.org/OAI/2.0/':
        mets_root_el = xml_tree.find('.//mets:mets', XMLNS)
        if mets_root_el is not None:
            return ET.ElementTree(mets_root_el).getroot()
    return None


def post_oai_extract_mets(the_self, the_data):
    """Just extract METS from OAI body"""

    xml_root = ET.fromstring(the_data)
    mets_tree = post_oai_extract_metsdata(xml_root)
    write_xml_file(mets_tree, the_self.path_mets, preamble=None)


def post_oai_store_ocr(path_local, the_data):
    """
    Store OCR XML as it is
    Explicite encoding is required with OCR-strings but not
    for byte objects like from semantics fulltext responses
    """

    if isinstance(the_data, str):
        the_data = the_data.encode('utf-8')
    xml_root = ET.fromstring(the_data)
    write_xml_file(xml_root, path_local, preamble=None)


def get_enclosed(tokens_str:str, mark_end='}', mark_start='{', func_find='rfind') -> str:
    """
    Search dict-like enclosed entry in string
    from end (rfind) or start (find)
    
    If no match, return empty string ''
    """
    if mark_end in tokens_str and mark_start in tokens_str:
        _offset_right_end = tokens_str.__getattribute__(func_find)(mark_end)
        _offset_right_start = tokens_str[:_offset_right_end].__getattribute__(func_find)(mark_start)
        _the_enclosed = tokens_str[_offset_right_start:(_offset_right_end+1)]
        return _the_enclosed
    return ''


class OAIRecord:
    """
    OAIRecord based on valid URN-Identifier with optional set specification data
    based on http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm

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
        self.set = UNSET
        self.date_stamp = UNSET
        self.info = UNSET
        self.state = UNSET
        self.state_datetime = UNSET

    @property
    def local_identifier(self):
        """
        source identifier for local usage
        as sub-directory in local storages
        * remove any 'oai:<host>...' related urn prefix
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
        return "{}\t{}\t{}\t{}\t{}\t{}".format(self.__urn, self.set,
                                               self.date_stamp, self.info, self.state, self.state_datetime)


def transform_statelist2oairecord(row):
    """Transform record from state list to Migration OAIRecord
    Sets _at least_ an identifier, optional
    * setSpec   record set(s) this record belongs to
    * datestamp time of publication or modification of record
    """

    record = OAIRecord(row[F_IDENTIFIER])
    if F_SPEC in row and str(row[F_SPEC]).strip():
        record.set = row[F_SPEC]
    if F_DATESTAMP in row and str(row[F_DATESTAMP]).strip():
        record.date_stamp = row[F_DATESTAMP]
    return record


def transform_to_record(row):
    """Serialize data row into OAIRecord
    with all attributes filled"""

    record = OAIRecord(row[F_IDENTIFIER])
    record.set = row[F_SPEC]
    record.date_stamp = row[F_DATESTAMP]
    record.info = row[F_STATE_INFO]
    record.state = row[F_STATE]
    record.state_datetime = row[F_STATE_TS]
    return record


class OAILoadException(Exception):
    """Load of OAI Data failed"""

    def __init__(self, msg):
        self.message = msg
        super().__init__(self.message)

class OAILoadServerError(OAILoadException):
    """Loading of Record via OAI failed due
    response status_code indicating Server Error"""


class OAILoadClientError(OAILoadException):
    """Loading of Record via OAI failed due 
    response status_code indicating Client Error"""


class OAILoadContentException(OAILoadException):
    """Loading of Record via OAI failed due unexpected 
    returned content, indicating missing record data 
    or even missing complete record"""


class OAIRecordHandlerException(Exception):
    """Mark Exception during procesing of OAI Record Lists"""

    def __init__(self, msg):
        self.message = msg
        super().__init__(self.message)


class OAIRecordCriteria:
    """
    Criteria to pick OAIRecords from a list
    """

    def matched(self, _: OrderedDict) -> bool:
        """Determine, whether given OAI-Record matched criteria"""


class OAIRecordCriteriaIdentifier(OAIRecordCriteria):

    def __init__(self, ident):
        self.ident = ident

    def matched(self, record: OrderedDict) -> bool:
        rec_id = record[F_IDENTIFIER]
        # maybe must deal shortened identifier (like legacy id)
        if 'oai' not in self.ident or ':' not in self.ident:
            rec_id = rec_id.split(':')[-1]
        return self.ident == rec_id


class OAIRecordCriteriaState(OAIRecordCriteria):

    def __init__(self, state):
        self.state = state

    def matched(self, record: OrderedDict) -> bool:
        record_state = record[F_STATE]
        return self.state == record_state


class OAIRecordCriteriaDatetime(OAIRecordCriteria):
    """
    Use datetime data to match OAIRecords.\n
    Match field 'STATE_TIME' (or 'CREATED')\n
    Default datetime pattern:
        'dt_pattern' = '%Y-%m-%d_%H:%M:%S' (fits 'STATE_TIME'),\n
        can be overwritten ('CREATED' uses commonly: '%Y-%m-%dT%H:%M:%SZ').
    """

    def __init__(self, **kwargs):
        self.dt_pattern = STATETIME_FORMAT
        self.field = F_STATE_TS
        self.dt_from = None
        self.dt_to = None
        # *ORDER MATTERS*
        if 'dt_field' in kwargs.keys():
            self.field = kwargs['dt_field']
        if 'dt_format' in kwargs.keys():
            self.dt_pattern = kwargs['dt_format']
        if 'dt_from' in kwargs.keys():
            self.dt_from = time.strptime(kwargs['dt_from'], self.dt_pattern)
        if 'dt_to' in kwargs.keys():
            self.dt_to = time.strptime(kwargs['dt_to'], self.dt_pattern)

    def matched(self, record: OrderedDict) -> bool:
        if self.field not in record:
            raise RuntimeError("Field {} not in {}".format(self.field, record))
        record_state_ts = record[self.field]
        if record_state_ts == RECORD_STATE_UNSET:
            return False
        record_ts = time.strptime(record_state_ts, self.dt_pattern)
        if self.dt_from and not self.dt_to:
            return record_ts >= self.dt_from
        elif self.dt_from and self.dt_to:
            return (record_ts >= self.dt_from) and (record_ts < self.dt_to)
        elif not self.dt_from and self.dt_to:
            return record_ts < self.dt_to
        return False


class OAIRecordCriteriaText(OAIRecordCriteria):

    def __init__(self, text, field=F_STATE_INFO) -> None:
        super().__init__()
        self.text = text
        self.field = field

    def matched(self, record: OrderedDict) -> bool:
        return self.text in record[self.field]


class OAIRecordHandler:
    """
    Process loading and storing of dataset records
    which contain information about the state of
    a single record
    * records has *at least* 3 fields:
    => first field/column identifies record
    => pre-last sets state
    => last field sets timestamp for state
    """

    def __init__(self, data_path, data_fields=None,
                 ident_field=0,
                 mark_open=RECORD_STATE_UNSET, mark_lock='busy',
                 transform_func=transform_statelist2oairecord):
        self.data_path = str(data_path)
        self.mark = {'open': mark_open, 'lock': mark_lock}
        self.position = None
        self.header = None
        self.transform_func = transform_func
        self._raw_lines = []
        # read raw lines
        with open(self.data_path, encoding='utf-8') as tmp:
            self._raw_lines = tmp.readlines()
        # pick rows containing *real* data
        # skip empty ones and comment lines
        data_lines = [line
                      for line in self._raw_lines
                      if self._is_data_row(line)]
        # check data format if possible
        self._restore_header(data_lines[0])
        if data_fields:
            self._validate_header(data_fields)
            self.header = data_fields
        # inspect data integrity
        self.ident_field = self.header[ident_field]
        self.state_field = self.header[-2]
        self.state_ts_field = self.header[-1]
        # build data
        self.index = {}
        self.data = []
        self.record_prefix = None
        self._build_data()

    @property
    def total_len(self):
        return len(self.data)

    def _build_data(self):
        """Transform raw_lines into meaningful data and build index
        for faster access time.

        with 0 = index of _raw_line_data
             1 = index of data dictionary
        """
        for i, _raw_row in enumerate(self._raw_lines):
            if self._is_data_row(_raw_row) and \
                    not self._is_header_row(_raw_row):
                _data = self._to_dict(_raw_row)
                _data_idx = len(self.data)
                self.data.append(_data)
                the_ident = _data[self.ident_field]
                self.index[the_ident] = (i, _data_idx)

    def _to_dict(self, row_as_str):
        """
        split row by tabulator
        """

        splits = row_as_str.strip().split('\t')
        return OrderedDict(zip(self.header, splits))

    @staticmethod
    def _to_str_nl(dict_row):
        return '\t'.join(dict_row.values()) + '\n'

    @staticmethod
    def _is_data_row(row):
        if row:
            row_str = row.strip()
            not_empty = len(row_str) > 0
            not_comment = not row_str.startswith(COMMENT_MARK)
            return not_empty and not_comment
        return False

    def _is_header_row(self, row_str):
        if self.header:
            return row_str.startswith(self.header[0])
        return False

    def _restore_header(self, first_line):
        _header = [h.strip() for h in first_line.split('\t')]
        if not _header:
            _header = HEADER_MIGRATION
        self.header = _header

    def _validate_header(self, data_fields):
        """validate both occurence and order"""
        if self.header != data_fields:
            msg = "invalid fields: '{}', expect: '{}'".format(
                self.header, data_fields)
            raise RuntimeError(msg)

    def next_record(self, state=None):
        """
        Get *NEXT* single OAIRecord _from scratch_ with
        given state if any exist, None otherwise
        """

        if not state:
            state = self.mark['open']
        for i, row in enumerate(self.data):
            if state == row[self.state_field]:
                self.position = "{:04d}/{:04d}".format((i + 1), self.total_len)
                return self.transform_func(row)

    def get(self, identifier, exact_match=True):
        """Read data for first OAIRecord with
        given identifier *without* changing state

        Args:
            identifier (string): record URN
            exact_match (bool) : identifier might just be contained or
                                 must match exaclty (default: True)
        """
        for i, row in enumerate(self.data):
            _ident = row[self.ident_field]
            if exact_match:
                if _ident == identifier:
                    return self.transform_func(row)
            else:
                if str(_ident).endswith(identifier):
                    return self.transform_func(row)
                elif identifier in _ident:
                    return self.transform_func(row)

    def save_record_state(self, identifier, state=None, **kwargs):
        """Mark Record state"""

        # read datasets
        if not state:
            state = self.mark['lock']
        if identifier in self.index.keys():
            (idx_raw, idx_data) = self.index[identifier]
            dict_row = self.data[idx_data]
            right_now = time.strftime(STATETIME_FORMAT)
            if kwargs:
                for k, v in kwargs.items():
                    dict_row[k] = v
            dict_row[self.state_field] = state
            dict_row[self.state_ts_field] = right_now
            self._raw_lines[idx_raw] = OAIRecordHandler._to_str_nl(dict_row)

        # if not existing_id:
        else:
            raise RuntimeError(
                'No Record for {} in {}! Cannot save state!'
                .format(identifier, self.data_path))

        # store actual state
        self._save_file()

    def _save_file(self):
        with open(self.data_path, 'w', encoding='utf-8', newline='')\
                as handle_write:
            handle_write.writelines(self._raw_lines)

    def states(self, criterias: list, set_state=RECORD_STATE_UNSET, 
        dry_run=True, verbose=False):
        """Process record states according certain criterias.

        Args:
            criterias (list): List of OAIRecordCriteria where each record
                must match all provided criterias. Defaults to a list 
                only containing OAIRecordCriteriaState(RECORD_STATE_UNSET).
            set_state (_type_, optional): Record state to set, if provided,
                and dry_run is disabled. Defaults to RECORD_STATE_UNSET.
            dry_run (bool, optional): Whether to persist possible
                modifications or just simulate. Defaults to True.
            verbose (bool, optional): Whether to list each record info instead of just 
                counting numbers. Defaults to False.
        """
        total_matches = []
        if len(criterias) == 0:
            criterias = [OAIRecordCriteriaState(RECORD_STATE_UNSET)]
        for record in self.data:
            if all(map(lambda c, d=record: c.matched(d), criterias)):
                if not dry_run:
                    record[self.state_field] = set_state
                    raw_index = self.index[record[self.ident_field]][0]
                    self._raw_lines[raw_index] = OAIRecordHandler._to_str_nl(record)
                total_matches.append(record)
        if not dry_run:
            self._save_file()
        if verbose:
            _report_stdout(total_matches)
        return len(total_matches)

    def frame(self, start, frame_size=1000, mark_state=RECORD_STATE_MASK_FRAME,
              sort_by=None) -> str:
        """
        create frame from OAI Records with start (inclusive)
        and opt frame_size (how many records from start)
        *please note*
        record count starts with "1", although intern represented as list
        """

        file_ext = None
        org_start = start
        if start > 0:
            start -= 1
        max_frame_size = len(self.data) - start
        end_frame = start + frame_size
        if frame_size > max_frame_size:
            end_frame = len(self.data)
        path_dir = os.path.abspath(os.path.dirname(self.data_path))
        file_name = os.path.basename(self.data_path)
        if '.' in file_name:
            file_name, file_ext = tuple(file_name.split('.'))
        if not file_ext:
            file_ext = 'csv'

        the_rows = []
        for i, row in enumerate(self.data):
            if i < start or i >= end_frame:
                row[F_STATE] = mark_state
            the_rows.append(row)

        # optional: sort by
        if sort_by is not None:
            if sort_by in self.header:
                the_rows = sorted(the_rows, key=lambda r: r[sort_by])
            else:
                raise RuntimeError("invalid sort by {}! only {} permitted!".format(
                    sort_by, self.header
                ))

        file_name_out = "{}_{:02d}_{:02d}.{}".format(
            file_name, org_start, end_frame, file_ext)
        path_out = os.path.join(path_dir, file_name_out)
        with open(path_out, 'w', encoding='UTF-8') as writer:
            csv_writer = csv.DictWriter(
                writer, delimiter='\t', fieldnames=self.header)
            csv_writer.writeheader()
            for row in the_rows:
                csv_writer.writerow(row)
        return path_out

    def merges(self, other_handler,
               other_require_state=None, other_ignore_state=RECORD_STATE_UNSET,
               append_unknown=True, dry_run=True, verbose=False) -> dict:
        """Merge record data into this list from other_list.
        Detected other_record with known identifier (via index) and
        in case of matching identifiers, merge *only* very first match.
        Merge only record data if source record is *not* in other_ignore_state.

        Precondition: both header fields must fit

        Args:
            other_handler (str|OAIRecordHandler):
                other OAIRecordHandler or str representation of file path
            other_require_state (str, default: None): only respect states from
                other_handler matching this state, if set. Can be used to
                compare or pick just specific outcome like 'migration_done',
                'ocr_done' or alike
            other_ignore_state (str, default: RECORD_STATE_UNSET): ignore record
                from other_handler with this state to preserve existing
                information in record from self_handler
            append_unknown (boolean, default:True): Whether append unknown
                record data or, if set 'False', leave them where they are
            dry_run (bool, default:True): Whether try/analyze or write result.
                Defaults to True, since it's a destructive operation.
            verbose (boolean, default:False): If turned on, list the records
                in detail instead of printing counts.

        Returns:
            Tuple: matched records, ignored records, new records in self from other
        """

        matches = []
        misses = []
        merges = []
        ignores = []
        requireds = []
        appendeds = []
        other_records = []
        if not isinstance(other_handler, OAIRecordHandler):
            other_handler = OAIRecordHandler(other_handler,
                                             data_fields=self.header)
        # check precondition
        if self.header != other_handler.header:
            raise OAIRecordHandlerException(
                f"Missmatch headers {self.header} != {other_handler.header}")

        other_records = other_handler.data
        # what might be integrated
        for other_record in other_records:
            other_ident = other_record[other_handler.ident_field]
            other_state = other_record[other_handler.state_field]
            # maybe merge
            if other_ident in self.index:
                idx_raw, idx_data = self.index[other_ident]
                self_record = self.data[idx_data]
                matches.append(other_record)
                if _is_unset(self_record) or _other_is_newer(self_record, other_record):
                    # preserve any existing record data
                    if other_ignore_state is not None and other_state == other_ignore_state:
                        ignores.append(other_record)
                        continue
                    if other_require_state is not None:
                        if other_state == other_require_state:
                            requireds.append(other_record)
                        else:
                            continue
                    # store
                    if not dry_run:
                        _merge(self_record, other_record)
                        merges.append(other_record)
                        self._raw_lines[idx_raw] = OAIRecordHandler._to_str_nl(self_record)
            # other_record previously unknown
            else:
                misses.append(other_record)
                # probably append at end
                if append_unknown:
                    appendeds.append(other_record)
                    if not dry_run:
                        next_idx_raw = len(self._raw_lines)
                        next_idx_data = len(self.data)
                        self._raw_lines.append(OAIRecordHandler._to_str_nl(other_record))
                        self.data.append(other_record)
                        self.index[other_ident] = (next_idx_raw, next_idx_data)
        if not dry_run:
            self._save_file()
        if verbose:
            print(f'### MATCHES ({len(matches)}) ###')
            _report_stdout(matches)
            print(f'### MERGES ({len(merges)}) ###')
            _report_stdout(merges)
            print(f'### MISSES ({len(misses)}) ###')
            _report_stdout(misses)
            print(f'### IGNORES ({len(ignores)}) ###')
            _report_stdout(ignores)
            print(f'### REQUIREDS ({len(requireds)}) ###')
            _report_stdout(requireds)
            print(f'### APPENDEDS ({len(appendeds)}) ###')
            _report_stdout(appendeds)
        return {'matches': len(matches), 'merges': len(merges),
                'misses': len(misses), 'ignores': len(ignores),
                'requireds': len(requireds), 'appendeds': len(appendeds)}


def _merge(self_record, other_record):
    self_record[F_STATE_INFO] = other_record[F_STATE_INFO]
    self_record[F_STATE] = other_record[F_STATE]
    self_record[F_STATE_TS] = other_record[F_STATE_TS]


def _is_unset(self_record):
    if self_record[F_STATE] == RECORD_STATE_UNSET:
        return True
    return False


def _other_is_newer(self_record, other_record):
    if self_record[F_STATE_TS] == RECORD_STATE_UNSET:
        return True
    else:
        dst_time = self_record[F_STATE_TS]
        src_time = other_record[F_STATE_TS]
        if src_time > dst_time:
            return True
        return False


def _report_stdout(list_records, delimiter='\t'):
    """Print set of OAIRecords, assume they are OrderedDicts
    with identical Headers"""
    if len(list_records) > 0:
        # read header
        _header = delimiter.join(list_records[0].keys())
        print('\n' + _header)
        for _r in list_records:
            print(delimiter.join(_r.values()))


class OAILoader:
    """
    Load OAI Records with corresponding metadata

    optional: post-process metadata XML after download
              to change hrefs, drop unwanted fileGroups, ...
              (default: None, i.e. no actions)
    optional: resolve linked resources like images and ocr-data
              store resources in specified directory layout
              (defaults: 'MAX' for images, 'FULLTEXT' for ocr)
              additional request arguments
              (defaults: empty dict)
    """

    def __init__(self, dir_local, base_url, **kwargs) -> None:
        self.dir_local = dir_local
        self.base_url = base_url
        self.groups = {}
        self.path_mets = None
        self.key_images = kwargs['group_images']\
            if 'group_images' in kwargs else 'MAX'
        self.post_oai = kwargs['post_oai'] if 'post_oai' in kwargs else None
        self.groups[self.key_images] = []
        self.key_ocr = kwargs['group_ocr']\
            if 'group_ocr' in kwargs else 'FULLTEXT'
        self.request_kwargs = kwargs['request_kwargs']\
            if 'request_kwargs' in kwargs else {}
        self.groups[self.key_ocr] = []
        self.store = None

    def load(self, record_identifier, local_dst, mets_digital_object_identifier=None,
             skip_resources=False, force_update=False, metadata_format='mets') -> int:
        """
        load metadata from OAI with optional caching in-between
        request additional linked resources if required

        * requires record_identifier to complete basic OAI request url
        * use mets_digital_object_identifier to get the proper MODS-section
          _if known_ for further inspection

        returns total of loaded metadata (1) plus number of
        additionally loaded resources
        """
        loaded = 0

        # sanitize url
        res_url = "{}?verb=GetRecord&metadataPrefix={}&identifier={}".format(
            self.base_url, metadata_format, record_identifier)
        self.path_mets = local_dst
        path_res = self._handle_load(res_url, self.path_mets, self.post_oai, force_update)
        if path_res:
            loaded += 1

        if skip_resources:
            return loaded

        # inspect if additional file resources are requested
        mets_reader = MetsReader(self.path_mets, mets_digital_object_identifier)

        # get linked resources
        for k in self.groups:
            self.groups[k] = mets_reader.get_filegrp_links(group=k)

        # if exist, download them too
        post_func = None
        for k, linked_res_urls in self.groups.items():
            if k == self.key_ocr:
                post_func = post_oai_store_ocr
            for linked_res_url in linked_res_urls:
                res_val_end = linked_res_url.split('/')[-1]
                res_val_path = self._calculate_path(k, res_val_end)
                if self._handle_load(linked_res_url, res_val_path, post_func):
                    loaded += 1
        return loaded

    def _handle_load(self, res_url, res_path, post_func, force_load=False):
        if self.store:
            stored_path = self.store.get(res_path)
            # if in store found ...
            if stored_path:
                if not force_load:
                    return None
                else:
                    # force update:
                    # 1. rename existing data
                    file_name = os.path.basename(stored_path)
                    file_dir = os.path.dirname(stored_path)
                    mets_ctime = str(int(os.stat(stored_path).st_mtime))
                    bkp_mets = file_name.replace('mets', mets_ctime)
                    os.rename(stored_path, os.path.join(file_dir, bkp_mets))
                    # 2. download again anyway
                    data_path = self.load_resource(res_url, res_path, post_func)
                    if data_path:
                        self.store.put(data_path)
                return res_path
            else:
                data_path = self.load_resource(res_url, res_path, post_func)
                if data_path:
                    self.store.put(data_path)
                return res_path
        else:
            return self.load_resource(res_url, res_path, post_func)

    def _calculate_path(self, *args):
        """
        calculate final path depending on some heuristics which
        fileGrp has been used - 'MAX' means images, not means 'xml'
        """
        res_path = os.path.join(str(self.dir_local), os.sep.join(list(args)))
        if '/MAX/' in res_path and not res_path.endswith('.jpg'):
            res_path += '.jpg'
        elif '/FULLTEXT/' in res_path and not res_path.endswith('.xml'):
            res_path += '.xml'
        return res_path

    def load_resource(self, url, path_local, post_func):
        """
        ensure local target dir exits and content can be written to
        optional: post-processing of received data if both exist
        """
        try:
            dst_dir = os.path.dirname(path_local)
            if not os.path.isdir(dst_dir):
                os.makedirs(dst_dir)
            local_path, data, content_type = request_resource(
                url, path_local, **self.request_kwargs)
            if post_func and data:
                # divide METS from XML (OCR-ALTO)
                # in rather brute force fashion
                _snippet = data[:512]
                # propably sanitize data, as it might originate
                # from test-data or *real* requests
                if not isinstance(_snippet, str):
                    _snippet = _snippet.decode('utf-8')
                if XMLNS['mets'] in _snippet or XMLNS['oai'] in _snippet:
                    data = post_func(self, data)
                elif 'http://www.loc.gov/standards/alto' in _snippet:
                    data = post_func(local_path, data)
                else:
                    raise OAILoadException(f"Can't handle {content_type} from {url}!")
            return local_path
        except OAILoadException as load_exc:
            raise load_exc
        except Exception as exc:
            msg = "processing '{}': {}".format(url, exc)
            raise RuntimeError(msg) from exc


class OAIFileSweeper:
    """ delete all resources (files and empty folder),
        except colorchecker, if any
        parse *.mets.xml to identify files to be deleted
    """

    def __init__(self, path_store, pattern='mets.xml', filegroups=['MAX', ]):
        self.work_dir = path_store
        self.pattern = pattern
        self.filegroups = filegroups if isinstance(filegroups, list)\
            else list(filegroups)

    def sweep(self):
        """remove OAI-Resources from given dir, if any contained"""

        work_dir = self.work_dir
        total = 0
        size = 0
        for curr_root, dirs, files in os.walk(work_dir):
            for filegroup in self.filegroups:
                if filegroup not in dirs:
                    continue

                curr_root_path = Path(curr_root)
                curr_filegroup_folder = curr_root_path / filegroup
                _files = [
                    f for f in curr_filegroup_folder.iterdir() if f.is_file()]

                if filegroup == 'MAX' and len(_files) < 2:
                    # One image only? This is likely the colorchecker! --> skip
                    # legacy
                    continue

                curr_mets_files = [
                    xml for xml in files if xml.endswith(self.pattern)]

                if curr_mets_files:
                    curr_mets_file = curr_mets_files[0]
                    curr_mets = curr_root_path.joinpath(curr_mets_file)
                    files_to_del = self._get_files(curr_mets, filegroup)

                    for pth in _files:
                        if pth.stem in files_to_del:
                            total += 1
                            size += pth.stat().st_size
                            try:
                                pth.unlink()
                                _parent = pth.parent
                                if _parent.is_dir() and\
                                   len(list(_parent.iterdir())) == 0:
                                    _parent.rmdir()
                            except PermissionError:
                                return 'cannot delete {} (insuff. permission)'\
                                    .format(pth)
        return (work_dir, total, "{} Mb".format(size >> 20))

    def _get_files(self, mets_xml, filegroup):
        xml_root = ET.parse(str(mets_xml)).getroot()
        xpath = ".//mets:fileGrp[@USE='{}']/mets:file/mets:FLocat".format(filegroup)
        locats = xml_root.findall(xpath, {"mets": "http://www.loc.gov/METS/"})
        links = [xl.get('{http://www.w3.org/1999/xlink}href') for xl in locats]
        return [Path(ln).stem for ln in links]


class LocalStore:
    """cache physical resources"""

    def __init__(self, dir_store_root, dir_local):
        self.dir_store_root = dir_store_root
        self.dir_local = dir_local

    def _calculate_path(self, path_res):
        if not isinstance(path_res, str):
            path_res = str(path_res)
        sub_path_res = path_res.replace(str(self.dir_local), '')
        if sub_path_res.startswith('/'):
            sub_path_res = sub_path_res[1:]
            if sub_path_res:
                return os.path.join(str(self.dir_store_root), sub_path_res)
        return None

    def get(self, path_res):
        """
        push resources by path into dst_dir, if
        they exist in store:
         * if single file requested, restore single file
         * if directory requested, restore from directory
        """

        path_store = self._calculate_path(path_res)
        if path_store and os.path.exists(path_store):
            dst_dir = os.path.dirname(path_res)
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir, exist_ok=True)
            if os.path.isfile(path_store):
                shutil.copy2(path_store, path_res)
            elif os.path.isdir(path_store):
                shutil.copytree(path_store, path_res)
            return path_store
        return None

    def put(self, path_res):
        """put single resource to path, for example created ocr data"""

        path_store = self._calculate_path(path_res)
        path_local_dir = os.path.dirname(path_store)
        if not os.path.exists(path_local_dir):
            os.makedirs(path_local_dir)
        # type cast to str required by python 3.5
        shutil.copy2(str(path_res), path_store)
        return path_store

    def put_all(self, src_dir, filter_ext='.xml'):
        """
        put all resources singular from last directory
        segement of current source directory to dst dir,
        if matching certain file_ext (default: "*.xml")
        """

        last_dir = os.path.basename(src_dir)
        dst_dir = os.path.join(self.dir_store_root, last_dir)
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        entries = os.listdir(src_dir)
        existing = [f
                    for f in entries
                    if str(f).endswith(filter_ext)]
        stored = 0
        for ent in existing:
            src = os.path.join(src_dir, ent)
            dst = os.path.join(dst_dir, ent)
            shutil.copy(src, dst)
            stored += 1
        return stored


def request_resource(url: str, path_local: Path, **kwargs):
    """
    request resources from provided url
    * textual content is interpreted as xml and
      passed back as string for further processing
    * binary image/jpeg content is stored at path_local
      passes back path_local
    * optional params (headers, cookies, ... ) forwarded as kwargs

    Raises Exception for unknown Content-Types and requests' Errors
    (https://docs.python-requests.org/en/master/_modules/requests/exceptions/).

    Returns Tuple (response_status, result)
    """

    status = 0
    result = None
    try:
        response = requests.get(url, **kwargs)
        status = response.status_code
        if status >= 400:
            _inf = "url '{}' returned '{}'".format(url, status)
            if status < 500:
                raise OAILoadClientError(_inf)
            else:
                raise OAILoadServerError(_inf)
        if status == 200:
            content_type = response.headers['Content-Type']

            # textual xml data
            if 'text' in content_type or 'xml' in content_type:
                result = response.content
                xml_root = ET.fromstring(result)
                check_error = xml_root.find('.//error', xml_root.nsmap)
                if check_error is not None:
                    msg = "the download of {} fails due to: '{}'".format(
                        url, check_error.text)
                    raise OAILoadException(msg)
                path_local = _sanitize_local_file_extension(
                    path_local, content_type)

            # catch other content types by MIMI sub_type
            # split "<application|image>/<sub_type>"
            elif content_type.split('/')[-1] in ['jpg', 'jpeg', 'pdf', 'png']:
                path_local = _sanitize_local_file_extension(
                    path_local, content_type)
                if not isinstance(path_local, Path):
                    path_local = Path(path_local)
                path_local.write_bytes(response.content)

            # if we went this far, something unexpected has been returned
            else:
                msg = "download {} with unhandled content-type {}".format(
                    url, content_type)
                raise OAILoadContentException(msg)
        return (path_local, result, content_type)
    except (OSError) as exc:
        msg = "fail to download '{}' to '{}'".format(url, path_local)
        raise RuntimeError(msg) from exc


def _sanitize_local_file_extension(path_local, content_type):
    if not isinstance(path_local, str):
        path_local = str(path_local)
    if 'xml' in content_type and not path_local.endswith('.xml'):
        path_local += '.xml'
    elif 'jpeg' in content_type and not path_local.endswith('.jpg'):
        path_local += '.jpg'
    elif 'png' in content_type and not path_local.endswith('.png'):
        path_local += '.png'
    elif 'pdf' in content_type and not path_local.endswith('.pdf'):
        path_local += '.pdf'
    return path_local


def get_smtp_server():
    return smtplib.SMTP('localhost')


def send_mail(subject, message, sender, recipients):
    """Notify recipients with message from local host
    subject: str The Subject
    message: str The Message
    sender: str Email of sender
    recipients: str, list  Email(s)
    """

    if isinstance(recipients, list):
        recipients = ','.join(recipients)
    try:
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipients + "\n"
        server = get_smtp_server()
        server.send_message(msg)
        server.quit()
        return "notification to '{}' sent: '{}'"\
               .format(recipients, message)
    except Exception as exc:
        msg = "Failed to notify '{}':'{}'\nmessage:\n'{}'\n{}!"\
            .format(recipients, subject, message, exc)
        return msg
