"""Record API"""

import ast
import csv
import collections
import os
import time


import digiflow.record as df_r

RECORD_STATE_MASK_FRAME = 'other_load'
SETSPEC_SPLITTER = '##'
STRING_QUOTES = "\"'"


class RecordHandlerException(Exception):
    """Mark Exception during procesing of Record List, i.e.
    handling corrupted due:
    * broken data/records
    * lists records invalid
    * specified seperator != expected columns
    * inconsistent header vs. actual columns
    """


class RecordHandler:
    """
    Process record with information about state
    * single records has *at least* 3 fields:
    => first field/column identifies record
    => pre-last sets state
    => last field sets timestamp for state
    """

    def __init__(self, data_path, data_fields=None,
                 ident_field=0,
                 mark_open=df_r.UNSET_LABEL, mark_lock='busy',
                 transform_func=df_r.row_to_record):
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
        """Number of records"""
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
        return collections.OrderedDict(zip(self.header, splits))

    @staticmethod
    def _to_str_nl(dict_row):
        return '\t'.join(dict_row.values()) + '\n'

    @staticmethod
    def _is_data_row(row):
        if row:
            row_str = row.strip()
            not_empty = len(row_str) > 0
            not_comment = not row_str.startswith(df_r.COMMENT_MARK)
            return not_empty and not_comment
        return False

    def _is_header_row(self, row_str):
        if self.header:
            return row_str.startswith(self.header[0])
        return False

    def _restore_header(self, first_line):
        _header = [h.strip() for h in first_line.split('\t')]
        if not _header:
            _header = df_r.LEGACY_HEADER
        self.header = _header

    def _validate_header(self, data_fields):
        """validate header fields presence and order"""
        if self.header != data_fields:
            msg = f"invalid fields: '{self.header}', expect: '{data_fields}'"
            raise RecordHandlerException(msg)

    def next_record(self, state=None):
        """
        Get *NEXT* Record with given state
        if any exist, otherwise None
        """

        if not state:
            state = self.mark['open']
        for i, row in enumerate(self.data):
            if self.state_field not in row:
                what = f"line:{i:03d} no {self.state_field} field {row}!"
                raise RecordHandlerException(what)
            if state == row[self.state_field]:
                self.position = f"{(i+1):04d}/{(self.total_len):04d}"
                return self.transform_func(row)
        return None

    def get(self, identifier, exact_match=True):
        """Read data for first Record with
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
        if identifier in self.index:
            (idx_raw, idx_data) = self.index[identifier]
            dict_row = self.data[idx_data]
            right_now = time.strftime(df_r.STATETIME_FORMAT)
            if kwargs:
                for k, v in kwargs.items():
                    dict_row[k] = v
            dict_row[self.state_field] = state
            dict_row[self.state_ts_field] = right_now
            self._raw_lines[idx_raw] = RecordHandler._to_str_nl(dict_row)

        # if not existing_id:
        else:
            raise RuntimeError(f'No Record for {identifier} in {self.data_path}! Cant save state!')

        # store actual state
        self._save_file()

    def _save_file(self):
        with open(self.data_path, 'w', encoding='utf-8', newline='')\
                as handle_write:
            handle_write.writelines(self._raw_lines)

    def states(self, criterias: list, set_state=df_r.UNSET_LABEL,
               dry_run=True, verbose=False):
        """Process record states according certain criterias.

        Args:
            criterias (list): List of RecordCriteria where each record
                must match all provided criterias. Defaults to a list 
                only containing RecordCriteriaState(RECORD_STATE_UNSET).
            set_state (_type_, optional): Record state to set, if provided,
                and dry_run is disabled. Defaults to RECORD_STATE_UNSET.
            dry_run (bool, optional): Whether to persist possible
                modifications or just simulate. Defaults to True.
            verbose (bool, optional): Whether to list each record info instead of just 
                counting numbers. Defaults to False.
        """
        total_matches = []
        if len(criterias) == 0:
            criterias = [df_r.State(df_r.UNSET_LABEL)]
        for record in self.data:
            if all(map(lambda c, d=record: c.matched(d), criterias)):
                if not dry_run:
                    record[self.state_field] = set_state
                    raw_index = self.index[record[self.ident_field]][0]
                    self._raw_lines[raw_index] = RecordHandler._to_str_nl(record)
                total_matches.append(record)
        if not dry_run:
            self._save_file()
        if verbose:
            _report_stdout(total_matches)
        return len(total_matches)

    def frame(self, start, frame_size=1000, mark_state=RECORD_STATE_MASK_FRAME,
              sort_by=None) -> str:
        """
        create record frame with start (inclusive)
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
                row[df_r.FIELD_STATE] = mark_state
            the_rows.append(row)

        # optional: sort by
        if sort_by is not None:
            if sort_by in self.header:
                the_rows = sorted(the_rows, key=lambda r: r[sort_by])
            else:
                raise RuntimeError(f"invalid sort by {sort_by}! only {self.header} permitted!")

        file_name_out = f"{file_name}_{org_start:02d}_{end_frame:02d}.{file_ext}"
        path_out = os.path.join(path_dir, file_name_out)
        with open(path_out, 'w', encoding='UTF-8') as writer:
            csv_writer = csv.DictWriter(
                writer, delimiter='\t', fieldnames=self.header)
            csv_writer.writeheader()
            for row in the_rows:
                csv_writer.writerow(row)
        return path_out

    def merges(self, other_handler,
               other_require_state=None, other_ignore_state=df_r.UNSET_LABEL,
               append_unknown=True, dry_run=True, verbose=False) -> dict:
        """Merge record data into this list from other_list.
        Detected other_record with known identifier (via index) and
        in case of matching identifiers, merge *only* very first match.
        Merge only record data if source record is *not* in other_ignore_state.

        Precondition: both header fields must fit

        Args:
            other_handler (str|RecordHandler):
                other RecordHandler or str representation of file path
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
        if not isinstance(other_handler, RecordHandler):
            other_handler = RecordHandler(other_handler,
                                          data_fields=self.header)
        # check precondition
        if self.header != other_handler.header:
            raise RecordHandlerException(
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
                        self._raw_lines[idx_raw] = RecordHandler._to_str_nl(self_record)
            # other_record previously unknown
            else:
                misses.append(other_record)
                # probably append at end
                if append_unknown:
                    appendeds.append(other_record)
                    if not dry_run:
                        next_idx_raw = len(self._raw_lines)
                        next_idx_data = len(self.data)
                        self._raw_lines.append(RecordHandler._to_str_nl(other_record))
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
    self_record[df_r.FIELD_STATE] = other_record[df_r.FIELD_STATE]
    self_record[df_r.FIELD_STATETIME] = other_record[df_r.FIELD_STATETIME]
    try:
        self_info = ast.literal_eval(_clear_trailing_quotes(self_record[df_r.FIELD_INFO]))
        other_info = ast.literal_eval(_clear_trailing_quotes(other_record[df_r.FIELD_INFO]))
        self_info.update(other_info)
        self_record[df_r.FIELD_INFO] = str(self_info)
    except (SyntaxError, ValueError):
        self_record[df_r.FIELD_INFO] = other_record[df_r.FIELD_INFO]

def _clear_trailing_quotes(raw_string:str):
    """Remove evil trailing chars like double/single 
    quotation marks"""

    if raw_string[0] in STRING_QUOTES:
        raw_string = raw_string[1:]
    if raw_string[-1] in STRING_QUOTES:
        raw_string = raw_string[:-1]
    return raw_string


def _is_unset(self_record):
    if self_record[df_r.FIELD_STATE] == df_r.UNSET_LABEL:
        return True
    return False


def _other_is_newer(self_record, other_record):
    if self_record[df_r.FIELD_STATETIME] == df_r.UNSET_LABEL:
        return True
    else:
        dst_time = self_record[df_r.FIELD_STATETIME]
        src_time = other_record[df_r.FIELD_STATETIME]
        if src_time > dst_time:
            return True
        return False


def _report_stdout(list_records, delimiter='\t'):
    """Print set of Records, assume they are OrderedDicts
    with identical Headers"""
    if len(list_records) > 0:
        # read header
        _header = delimiter.join(list_records[0].keys())
        print('\n' + _header)
        for _r in list_records:
            print(delimiter.join(_r.values()))
