"""API for handling records with server/client mechanics"""

import dataclasses
import functools
import http.server
import json
import logging
import sys

from pathlib import Path

import requests

import digiflow as df
import digiflow.record as df_r


DEFAULT_COMMAND_NEXT = 'next'
DEFAULT_COMMAND_UPDATE = 'update'
_MIME_TXT = 'text/plain'
DEFAULT_MARK_BUSY = 'busy'

X_HEADER_GET_STATE = 'X-GET-STATE'
X_HEADER_SET_STATE = 'X-SET-STATE'

STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

DATA_EXHAUSTED_PREFIX = 'no open records'
DATA_EXHAUSTED_MARK = DATA_EXHAUSTED_PREFIX + ' in {}'


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


class RecordsExhaustedException(Exception):
    """Mark state when no more records can be
    achieved anymore"""


class RecordRequestHandler(http.server.SimpleHTTPRequestHandler,
                           df.FallbackLogger):
    """Simple handler for POST and GET requests
    without additional security - use at own risk
    """

    def __init__(self, start_info: HandlerInformation,
                 *args,
                 **kwargs):
        self.record_list_directory: Path = start_info.data_path
        self.logger = start_info.logger
        self.command_next = DEFAULT_COMMAND_NEXT
        self.command_update = DEFAULT_COMMAND_UPDATE
        super(http.server.SimpleHTTPRequestHandler, self).__init__(*args, **kwargs)

    def do_GET(self):
        """handle GET request"""
        client_name = self.address_string()
        if self.path == '/favicon.ico':
            return
        self.log("request '%s' from client %s", self.path, client_name, level=logging.DEBUG)
        get_record_state = self.headers.get(X_HEADER_GET_STATE)
        set_record_state = self.headers.get(X_HEADER_SET_STATE)
        try:
            _, file_name, command = self.path.split('/')
        except ValueError:
            self.wfile.write(
                b'please provide record file name and command '
                b' e.g.: /oai_record_vd18/next')
            self.log("missing data: '%s'", self.path, level=logging.WARNING)
            return
        if command == DEFAULT_COMMAND_NEXT:
            state, data = self.get_next_record(file_name, client_name,
                                               get_record_state, set_record_state)
            self.log("deliver next record: '%s'", data, level=logging.DEBUG)
            if isinstance(data, str):
                self._set_headers(state, _MIME_TXT)
                self.wfile.write(data.encode('utf-8'))
            else:
                self._set_headers(state)
                self.wfile.write(json.dumps(data, default=df_r.Record.dict).encode('utf-8'))

    def do_POST(self):
        """handle POST request"""
        data = 'no data available'
        client_name = self.address_string()
        self.log('url path %s from %s', self.path, client_name)
        try:
            _, file_name, command = self.path.split('/')
        except ValueError as val_err:
            self.wfile.write(
                b'please provide record file name and command '
                b' e.g.: /records-vd18/next')
            self.log('request next record failed %s', val_err.args[0], level=logging.ERROR)
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data_dict = json.loads(post_data)
        self.log("POST request, Path: %s", self.path, level=logging.DEBUG)
        self.log('do_POST: %s', data_dict)
        if command == DEFAULT_COMMAND_UPDATE:
            ident = data_dict.get(df_r.FIELD_IDENTIFIER)
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
                self.wfile.write(f"no entry for {ident} in {file_name}!".encode('utf-8'))

    def _set_headers(self, state=200, mime_type='application/json') -> None:
        self.send_response(state)
        self.send_header('Content-type', mime_type)
        self.end_headers()

    def get_data_file(self, data_file_name: str):
        """data_file_name comes with no extension!
           so we must search for a valid match-
           returns propably None-values if
           nothing found.
        """
        if isinstance(data_file_name, str):
            data_file_name = Path(data_file_name).stem
        for a_file in self.record_list_directory.iterdir():
            if data_file_name == Path(a_file).stem:
                data_file = self.record_list_directory / a_file.name
                return data_file
        self.log("found no %s in %s", data_file_name, self.record_list_directory,
                 level=logging.CRITICAL)
        return None

    def get_next_record(self, file_name, client_name, requested_state, set_state) -> tuple:
        """Deliver next record data if both
        * in watched directory exists record list matching file_name
        * inside this record list are open records available
        """

        self.log("look for %s in %s", file_name, self.record_list_directory)
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
            the_msg = f'{DATA_EXHAUSTED_MARK}: {data_file_path}'
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
            self.log('do_POST: %s not found', data_file_path, level=logging.ERROR)
            return (404, f"data file not found: {data_file_path}")
        try:
            handler = df_r.RecordHandler(data_file_path)
            if isinstance(in_data, dict):
                in_data = df_r.Record.parse(in_data)
            in_ident = in_data.identifier
            prev_record: df_r.Record = handler.get(in_ident)
            prev_record.info = in_data.info
            info_str = f"{prev_record.info}"
            handler.save_record_state(in_ident,
                                      state=in_data.state, **{df_r.FIELD_INFO: info_str})
            _msg = f"update done for {in_ident} in '{data_file_path}"
            self.log(_msg)
            return (200, _msg)
        except RuntimeError as _rer:
            _msg = f"update fail for {in_ident} in '{data_file_path}' ({_rer.args[0]})"
            self.log(_msg, level=logging.ERROR)
            return (500, _msg)


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
        super().__init__(some_logger=logger)

    def get_record(self, get_record_state, set_record_state):
        """Request Record from service and de-serialize
        json encoded content into record object
        """
        try:
            the_headers = {X_HEADER_GET_STATE: get_record_state,
                           X_HEADER_SET_STATE: set_record_state}
            response = requests.get(f'{self.oai_server_url}/next',
                                    timeout=300, headers=the_headers)
        except requests.exceptions.RequestException as err:
            if self.logger is not None:
                self.logger.error("connection fails: %s", err)
            sys.exit(1)
        status = response.status_code
        result = response.content
        if status == 404:
            # probably nothing to do?
            if DATA_EXHAUSTED_MARK in str(result):
                if self.logger is not None:
                    self.logger.info(result)
                raise RecordsExhaustedException(result.decode(encoding='utf-8'))
            # otherwise exit anyway
            sys.exit(1)

        if status != 200:
            if self.logger is not None:
                self.logger.error(
                    "server connection status: %s -> %s", status, result)
            sys.exit(1)

        # otherwise response ok
        self.record = df_r.Record.parse(response.json())
        return self.record

    def update(self, status, oai_urn, **kwargs):
        """Store status update && send message to OAI Service"""
        if self.logger is not None:
            self.logger.debug("set status '%s' for urn '%s'", status, oai_urn)
        self.record = df_r.Record(oai_urn)
        self.record.state = status
        # if we have to report somethin' new, then append it
        if kwargs is not None and len(kwargs) > 0:
            try:
                self.record.info = kwargs
            except AttributeError as attr_err:
                self.logger.error("info update failed for %s: %s (prev:%s, in:%s)",
                                  self.record.identifier,
                                  attr_err.args[0],
                                  self.record.info, kwargs)
        if self.logger is not None:
            self.logger.debug("update record %s url %s", self.record, self.oai_server_url)
        return requests.post(f'{self.oai_server_url}/update', json=self.record.dict(), timeout=60)


def run_server(host, port, start_data: HandlerInformation):
    """start server to process requests
    for local file resources"""

    the_logger = start_data.logger
    the_logger.info("server starts listen at: %s:%s", host, port)
    the_logger.info("deliver record files from: %s", start_data.data_path)
    the_logger.info("call next record with: %s:%s/<record-file>/next", host, port)
    the_logger.info("post update data with: %s:%s/<record-file>/update", host, port)
    the_logger.info("stop server press CTRL+C")
    the_handler = functools.partial(RecordRequestHandler, start_data)
    with http.server.HTTPServer((host, int(port)), the_handler) as the_server:
        try:
            the_server.serve_forever(5.0)
        except KeyboardInterrupt:
            the_server.shutdown()
    the_logger.info("shutdown record server (%s:%s)", host, port)
