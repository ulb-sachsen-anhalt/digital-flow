"""API for handling records with server/client mechanics"""

import dataclasses
import functools
import http.server
import json
import logging

from pathlib import Path

import requests

import digiflow as df
import digiflow.record.common as df_rc
import digiflow.record as df_r


DEFAULT_COMMAND_NEXT = 'next'
DEFAULT_COMMAND_UPDATE = 'update'
_MIME_TXT = 'text/plain'
DEFAULT_MARK_BUSY = 'busy'

X_HEADER_GET_STATE = 'X-GET-STATE'
X_HEADER_SET_STATE = 'X-SET-STATE'

STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

DATA_EXHAUSTED_PREFIX = 'no records '
DATA_EXHAUSTED_MARK = DATA_EXHAUSTED_PREFIX + '{} in {}'


@dataclasses.dataclass
class HandlerInformation:
    """Encapsulate some basic
    information needed to do 
    the handling"""

    data_path: Path
    logger: logging.Logger

    def __init__(self, data_path, logger):
        """Enforce proper types"""
        if isinstance(data_path, str):
            self.data_path = Path(data_path)
        if not self.data_path.is_absolute():
            self.data_path = self.data_path.resolve()
        self.logger = logger


class RecordsExhaustedException(df_rc.RecordDataException):
    """Mark state when no more records can be
    achieved anymore"""


class RecordsServiceException(df_rc.RecordDataException):
    """Mark generic exception state"""


class RecordRequestHandler(http.server.SimpleHTTPRequestHandler,
                           df.FallbackLogger):
    """Simple handler for POST and GET requests
    without additional security - use at own risk
    """

    def __init__(self, start_info: HandlerInformation,
                 *args,
                 **kwargs):
        self.record_list_directory: Path = start_info.data_path
        self.command_next = DEFAULT_COMMAND_NEXT
        self.command_update = DEFAULT_COMMAND_UPDATE
        self._logger = start_info.logger
        super(http.server.SimpleHTTPRequestHandler, self).__init__(*args, **kwargs)

    def _parse_request_path(self):
        try:
            _, file_name, command = self.path.split('/')
            return command, file_name
        except ValueError:
            self._set_headers(state=400, mime_type=_MIME_TXT)
            self.wfile.write(
                b'provide file name and command, e.g.: /<file_name>/<command>')
            self.log("unable to parse '%s'", self.path, level=logging.ERROR)

    def do_GET(self):
        """handle GET request"""
        client_name = self.address_string()
        get_record_state = self.headers.get(X_HEADER_GET_STATE)
        set_record_state = self.headers.get(X_HEADER_SET_STATE)
        command, file_name = self._parse_request_path()
        if command is not None and command == DEFAULT_COMMAND_NEXT:
            state, data = self.get_next_record(file_name, client_name,
                                               get_record_state, set_record_state)
            self.log("get '%s': '%s'", get_record_state, data,
                     level=logging.DEBUG)
            if isinstance(data, str):
                self._set_headers(state, _MIME_TXT)
                self.wfile.write(data.encode('utf-8'))
            else:
                self._set_headers(state)
                self.wfile.write(json.dumps(data, default=df_r.Record.dict).encode('utf-8'))

    def log_request(self, _):
        """silence internal logger"""

    def do_POST(self):
        """handle POST request"""
        client_name = self.address_string()
        self.log('url path %s from %s', self.path, client_name)
        command, file_name = self._parse_request_path()
        if command is None:
            return
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data_dict = json.loads(post_data)
        ident = data_dict.get(df_r.FIELD_IDENTIFIER)
        if command == DEFAULT_COMMAND_UPDATE:
            self.log('update %s in %s: %s', ident, self.path, data_dict)
            if ident:
                state, data = self.update_record(file_name, data_dict)
                if isinstance(data, str):
                    self._set_headers(state, _MIME_TXT)
                    self.wfile.write(data.encode('utf-8'))
                else:
                    self._set_headers(state)
                    self.wfile.write(json.dumps(data, default=data.dict).encode('utf-8'))
            else:
                self._set_headers(404, _MIME_TXT)
                self.wfile.write(f"no {ident} in {file_name}!".encode('utf-8'))

    def _set_headers(self, state=200, mime_type='application/json') -> None:
        self.send_response(state)
        self.send_header('Content-type', mime_type)
        self.end_headers()

    def get_data_file(self, file_name: str):
        """data_file_name comes with no extension!
           so we must search for a valid match-
           returns propably None-values if
           nothing found.
        """
        if isinstance(file_name, str):
            file_name = Path(file_name).stem
        for a_file in self.record_list_directory.iterdir():
            if file_name == Path(a_file).stem:
                data_file = self.record_list_directory / a_file.name
                return data_file
        self.log("no file %s in %s", file_name, self.record_list_directory,
                 level=logging.CRITICAL)

    def get_next_record(self, file_name, client_name, requested_state, set_state) -> tuple:
        """Deliver next record data if both
        * in watched directory exists record list matching file_name
        * inside this record list are open records available
        """

        self.log("get %s from %s", file_name, self.record_list_directory)
        data_file_path = self.get_data_file(file_name)
        # no match results in 404 - resources not available after all
        if data_file_path is None:
            self.log("no '%s' found in '%s'", file_name, self.record_list_directory,
                     level=logging.WARNING)
            return (404, f"no file '{file_name}' in {self.record_list_directory}")

        handler = df_r.RecordHandler(data_file_path, transform_func=df_r.row_to_record)
        next_record = handler.next_record(requested_state)
        # if no record available, alert no resource
        if next_record is None:
            the_msg = DATA_EXHAUSTED_MARK.format(data_file_path)
            self.log(the_msg)
            return (404, the_msg)

        # store information which client got the package delivered
        client_info = {'client': client_name}
        next_record.info = client_info
        handler.save_record_state(
            next_record.identifier, set_state, **{df_r.FIELD_INFO: f'{next_record.info}'})
        return (200, next_record)

    def update_record(self, data_file, in_data) -> tuple:
        """write data dict send by client
        throws RuntimeError if record to update not found
        """

        data_file_path = self.get_data_file(data_file)
        if data_file_path is None:
            self.log('%s not found', data_file_path, level=logging.ERROR)
            return (404, f"file not found: {data_file_path}")
        try:
            handler = df_r.RecordHandler(data_file_path)
            if isinstance(in_data, dict):
                in_data = df_r.Record.parse(in_data)
            in_ident = in_data.identifier
            prev_record: df_r.Record = handler.get(in_ident)
            prev_record.info = in_data.info
            info_str = f"{prev_record.info}"
            in_state = in_data.state
            handler.save_record_state(in_ident,
                                      state=in_state, **{df_r.FIELD_INFO: info_str})
            msg = f"set {in_ident} to {in_state} in {data_file_path}"
            self.log(msg)
            return (200, msg)
        except RuntimeError as _rer:
            msg = f"set {in_ident} to {in_state} in '{data_file_path}' failed: {_rer.args[0]}"
            self.log(msg, level=logging.ERROR)
            return (500, msg)


class Client(df.FallbackLogger):
    """Implementation of OAI Service client with
    capabilities to get next OAI Record data
    and communicate results (done|fail)
    """

    def __init__(self, oai_record_list_label, host, port,
                 logger=None):
        self.oai_record_list_label = oai_record_list_label
        self.record: df_r.Record = None
        self.oai_server_url = f'http://{host}:{port}/{oai_record_list_label}'
        self.timeout_secs = 30
        super().__init__(some_logger=logger)

    def get_record(self, get_record_state, set_record_state):
        """Request Record from service and de-serialize
        json encoded content into record object
        """
        try:
            the_headers = {X_HEADER_GET_STATE: get_record_state,
                           X_HEADER_SET_STATE: set_record_state}
            response = requests.get(f'{self.oai_server_url}/next',
                                    timeout=self.timeout_secs, headers=the_headers)
        except requests.exceptions.RequestException as err:
            if self._logger is not None:
                self._logger.error("Connection failure: %s", err)
            raise RecordsServiceException(f"Connection failure: {err}") from err
        status = response.status_code
        result = response.content
        if status == 404:
            # probably nothing to do?
            if DATA_EXHAUSTED_PREFIX in str(result):
                raise RecordsExhaustedException(result.decode(encoding='utf-8'))

        if status != 200:
            self.log("server connection status: %s -> %s", status, result,
                     logging.ERROR)
            raise RecordsServiceException(f"Record service error {status} - {result}")

        # otherwise response ok
        self.record = df_r.Record.parse(response.json())
        return self.record

    def update(self, status, oai_urn, **kwargs):
        """Store status update && send message to OAI Service"""
        self.log("set status '%s' for urn '%s'", status, oai_urn, logging.DEBUG)
        self.record = df_r.Record(oai_urn)
        self.record.state = status
        # if we have to report somethin' new, then append it
        if kwargs is not None and len(kwargs) > 0:
            try:
                self.record.info = kwargs
            except AttributeError as attr_err:
                self._logger.error("info update failed for %s: %s (prev:%s, in:%s)",
                                   self.record.identifier,
                                   attr_err.args[0],
                                   self.record.info, kwargs)
        self.log("update record %s url %s", self.record, self.oai_server_url, logging.DEBUG)
        return requests.post(f'{self.oai_server_url}/update', json=self.record.dict(), timeout=60)


def run_server(host, port, start_data: HandlerInformation):
    """start server to process requests
    for local file resources"""

    the_logger = start_data.logger
    the_logger.info("listen at: %s:%s for files from %s", host, port, start_data.data_path)
    the_logger.info("next data: %s:%s/<record-file>/next", host, port)
    the_logger.info("post data: %s:%s/<record-file>/update", host, port)
    the_logger.info("to stop server, press CTRL+C")
    the_handler = functools.partial(RecordRequestHandler, start_data)
    with http.server.HTTPServer((host, int(port)), the_handler) as the_server:
        try:
            the_server.serve_forever(5.0)
        except KeyboardInterrupt:
            the_server.shutdown()
    the_logger.info("shutdown record server (%s:%s)", host, port)
