"""API for handling records with server/client mechanics"""

import argparse
import configparser
import dataclasses
import functools
import http.server
import json
import logging
import logging.config
import logging.handlers
import os
import sys
import tempfile
import typing

from pathlib import Path

import requests

import digiflow as df
import digiflow.record.common as df_rc
import digiflow.record as df_r


DEFAULT_HEADER = {
    "Content-Type": "application/json"
}
TEXT_HEADER = {
    "Content-Type": "text/plain"
}
DEFAULT_COMMAND_NEXT = 'next'
DEFAULT_COMMAND_UPDATE = 'update'
DEFAULT_MARK_BUSY = 'busy'

X_HEADER_GET_STATE = 'X-GET-STATE'
X_HEADER_SET_STATE = 'X-SET-STATE'

STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

DATA_EXHAUSTED_PREFIX = "no records "
DATA_EXHAUSTED_MARK = DATA_EXHAUSTED_PREFIX + "{} in {}"

LOG_FORMATTER = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S")


@dataclasses.dataclass
class HandlerInformation:
    """Encapsulate some basic
    information needed to do 
    the handling"""

    data_path: Path
    logger: logging.Logger
    client_ips = []

    def __init__(self, data_path, logger):
        """Enforce proper types"""
        self.data_path = data_path
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


# make pylint accept names like "do_POST"
# pylint:disable=invalid-name
class RecordRequestHandler(http.server.SimpleHTTPRequestHandler,
                           df.FallbackLogger):
    """Simple handler for POST and GET requests
    without additional security - use at own risk
    """

    def __init__(self, start_info: HandlerInformation,
                 *args,
                 **kwargs):
        self.record_list_directory: Path = start_info.data_path
        self.client_ips = start_info.client_ips
        self.command_next = DEFAULT_COMMAND_NEXT
        self.command_update = DEFAULT_COMMAND_UPDATE
        self._logger = start_info.logger
        super(http.server.SimpleHTTPRequestHandler, self).__init__(*args, **kwargs)

    def _parse_request_path(self):
        try:
            _, file_name, command = self.path.split('/')
            return command, file_name
        except (ValueError, TypeError):
            self._respond(state=400, headers=TEXT_HEADER)
            self.wfile.write(
                b'provide file name and command, e.g.: /<file_name>/<command>')
            self.log("unable to parse '%s'", self.path, level=logging.ERROR)
        return None

    def _client_allowed(self):
        """check incomming requests verso list of allowed
        client ips if available"""
        if self.client_ips is not None and len(self.client_ips) > 0:
            client_name = self.address_string()
            is_allowed = client_name in self.client_ips
            if not is_allowed:
                self._respond(state=404, headers={})
                return False
        return True

    def do_GET(self):
        """handle GET request"""
        client_name = self.address_string()
        if not self._client_allowed():
            self.log("request from %s rejected", self.address_string(), level=logging.WARNING)
            return
        get_record_state = self.headers.get(X_HEADER_GET_STATE, failobj=df.UNSET_LABEL)
        set_record_state = self.headers.get(X_HEADER_SET_STATE)
        parsed_request = self._parse_request_path()
        if isinstance(parsed_request, tuple):
            command, file_name = parsed_request
            if command is not None and command == DEFAULT_COMMAND_NEXT:
                state, data = self.get_next_record(file_name, client_name,
                                                   get_record_state, set_record_state)
                if state != 200:
                    self._respond(state)
                    self.wfile.write(data.encode('utf-8'))
                    return
                if isinstance(data, df_r.Record):
                    ident = data.identifier
                elif isinstance(data, dict):
                    ident = data.get(df_r.FIELD_IDENTIFIER)
                self.log("set record %s in %s to %s (load: '%s')",
                         ident, self.path, client_name, data)
                if isinstance(data, str):
                    self._respond(state, headers=TEXT_HEADER)
                    self.wfile.write(data.encode('utf-8'))
                else:
                    self._respond(state)
                    self.wfile.write(json.dumps(data, default=df_r.Record.dict).encode('utf-8'))

    def log_request(self, *_):
        """silence internal logger"""

    def do_POST(self):
        """handle POST request"""
        client_name = self.address_string()
        if not self._client_allowed():
            self.log("request from %s rejected", self.address_string(), level=logging.WARNING)
            return
        self.log('url path %s from %s', self.path, client_name,
                 level=logging.INFO)
        parsed_request = self._parse_request_path()
        if isinstance(parsed_request, tuple):
            command, file_name = parsed_request
            if command is None:
                return
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data_dict = json.loads(post_data)
            ident = data_dict.get(df_r.FIELD_IDENTIFIER)
            if DEFAULT_COMMAND_UPDATE == command:
                self.log("set record %s in %s by %s (load: '%s')",
                         ident, self.path, client_name, data_dict)
                if ident:
                    state, data = self.update_record(file_name, data_dict)
                    if isinstance(data, str):
                        self._respond(state, TEXT_HEADER)
                        self.wfile.write(data.encode('utf-8'))
                    else:
                        self._respond(state)
                        self.wfile.write(json.dumps(data, default=data.dict).encode('utf-8'))
                else:
                    self._respond(404, TEXT_HEADER)
                    self.wfile.write(f"no {ident} in {file_name}!".encode('utf-8'))

    def _respond(self, state=200, headers=None) -> None:
        self.send_response(state)
        if headers is None:
            headers = DEFAULT_HEADER
        for k, v in headers.items():
            self.send_header(k, v)
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
                 level=logging.WARNING)
        return None

    def get_next_record(self, file_name, client_name, requested_state, set_state) -> tuple:
        """Deliver next record data if both
        * in watched directory exists record list matching file_name
        * inside this record list are open records available
        """

        data_file_path = self.get_data_file(file_name)
        # no match results in 404 - resources not available after all
        if data_file_path is None:
            self.log("no file %s found in %s", file_name, self.record_list_directory,
                     level=logging.WARNING)
            return (404, f"no '{file_name}' in {self.record_list_directory}")

        handler = df_r.RecordHandler(data_file_path, transform_func=df_r.row_to_record)
        next_record = handler.next_record(requested_state)
        # if no record available, alert no resource
        if next_record is None:
            the_msg = DATA_EXHAUSTED_MARK.format(requested_state, data_file_path)
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
            return (404, f"{data_file_path} not found")
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
            msg = f"set {in_ident} to {in_state} in {data_file_path} failed: {_rer.args[0]}"
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
            self.log("Connection failure: %s", err, level=logging.ERROR)
            raise RecordsServiceException(f"Connection failure: {err}") from err
        status = response.status_code
        result = response.content
        if status == 404:
            # probably nothing to do?
            if DATA_EXHAUSTED_PREFIX in str(result):
                raise RecordsExhaustedException(result.decode(encoding='utf-8'))

        if status != 200:
            if result is None or len(result) == 0:
                result = df.UNSET_LABEL
            self.log("server connection status: %s -> %s", status, result,
                     level=logging.ERROR)
            raise RecordsServiceException(f"Record service error {status} - {result}")

        # otherwise response ok
        self.record = df_r.Record.parse(response.json())
        return self.record

    def update(self, status, oai_urn, **kwargs):
        """Store status update && send message to OAI Service"""
        self.log("set status '%s' for urn '%s'", status, oai_urn,
                 level=logging.DEBUG)
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
        self.log("update record %s url %s", self.record, self.oai_server_url,
                 level=logging.DEBUG)
        return requests.post(f'{self.oai_server_url}/update', json=self.record.dict(), timeout=60)


def run_server(host, port, start_data: HandlerInformation):
    """start server to process requests
    for local file resources"""

    the_logger = start_data.logger
    the_logger.info("listen at: %s:%s for files from %s", host, port, start_data.data_path)
    if start_data.client_ips and len(start_data.client_ips) > 0:
        the_logger.info("accept requests only from %s", start_data.client_ips)
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


def _set_file_handler(logfile_path: Path):
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            logfile_path, maxBytes=1_048_576)
    except (PermissionError, FileNotFoundError):
        tmp_log_file = os.path.join(tempfile.gettempdir(), logfile_path)
        file_handler = logging.handlers.RotatingFileHandler(
            tmp_log_file, maxBytes=10_485_760)
    file_handler.setFormatter(LOG_FORMATTER)
    return file_handler


if __name__ == "__main__":
    ARGP = argparse.ArgumentParser()
    ARGP.add_argument("-c", "--config", required=False)
    ARGP.add_argument("--host", required=False)
    ARGP.add_argument("--port", required=False)
    ARGP.add_argument("--log_dir", required=False)
    ARGP.add_argument("--log_filename", required=False)
    ARGP.add_argument("--log_config", required=False)
    ARGP.add_argument("--logger_name", required=False)
    ARGP.add_argument("--accepted_ips", required=False)
    ARGP.add_argument("--resources_dir", required=False)
    THE_ARGS: typing.Dict = vars(ARGP.parse_args())

    THE_CONFIG = configparser.ConfigParser()
    THE_CONFIG.add_section("log")
    THE_CONFIG.add_section("server")
    READ_CONFIG_FILES = []
    if THE_ARGS["config"] is not None:
        CFG_FILE = Path(THE_ARGS["config"])
        if not CFG_FILE.is_file():
            raise RuntimeError(f"config file missing: {CFG_FILE.resolve()}")
        READ_CONFIG_FILES = THE_CONFIG.read(CFG_FILE)
    if THE_ARGS["log_dir"] is not None:
        THE_CONFIG.set("log", "log_dir", THE_ARGS["log_dir"])
    if THE_ARGS["log_filename"] is not None:
        THE_CONFIG.set("log", "log_filename", THE_ARGS["log_filename"])
    if THE_ARGS["log_config"] is not None:
        THE_CONFIG.set("log", "log_config", THE_ARGS["log_config"])
    if THE_ARGS["logger_name"] is not None:
        THE_CONFIG.set("log", "logger_name", THE_ARGS["logger_name"])
    if THE_ARGS["host"] is not None:
        THE_CONFIG.set("server", "host", THE_ARGS["host"])
    if THE_ARGS["port"] is not None:
        THE_CONFIG.set("server", "port", THE_ARGS["port"])
    if THE_ARGS["accepted_ips"] is not None:
        THE_CONFIG.set("server", "accepted_ips", THE_ARGS["accepted_ips"])
    if THE_ARGS["resources_dir"] is not None:
        THE_CONFIG.set("server", "resources_dir", THE_ARGS["resources_dir"])

    # all options present?
    try:
        RAW_LOG_DIR = THE_CONFIG.get("log", "log_dir")
        LOG_FILENAME = THE_CONFIG.get("log", "log_filename")
        LOGGER_NAME = THE_CONFIG.get("log", "logger_name")
        RES_DIR = THE_CONFIG.get("server", "resources_dir")
    except configparser.NoOptionError as err:
        raise RuntimeError(f"Configuration option missing: {err.args[0]}") from err

    # check pre-conditions
    if not os.access(RAW_LOG_DIR, os.F_OK and os.W_OK):
        raise RuntimeError(f"cant store log files at non-writeable directory: {RAW_LOG_DIR}")
    LOG_DIR = Path(RAW_LOG_DIR)
    LOG_PATH = (LOG_DIR / LOG_FILENAME).resolve()
    # foster record directory path
    if '~' in RES_DIR:
        RES_DIR = Path(RES_DIR).expanduser()
    RES_DIR = Path(RES_DIR).absolute().resolve()

    # now initialize server side logging
    USED_LOGGING_CONF = None
    if THE_ARGS["log_config"] is not None:
        LOG_CONFIG_FILE = Path(THE_ARGS["log_config"]).resolve()
        CFG_DICT = {'logname': str(LOG_PATH)}
        logging.config.fileConfig(str(LOG_CONFIG_FILE), defaults=CFG_DICT)
        USED_LOGGING_CONF = LOG_CONFIG_FILE
    LOGGER = logging.getLogger(LOGGER_NAME)
    if USED_LOGGING_CONF is None:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(LOG_FORMATTER)
        file_handler = _set_file_handler(LOG_PATH)
        LOGGER.addHandler(console_handler)
        LOGGER.addHandler(file_handler)
        LOGGER.setLevel(logging.DEBUG)
    LOG_TAG = Path(__file__).stem
    if len(READ_CONFIG_FILES) > 0:
        LOGGER.debug("[%s] uses module configuration %s", LOG_TAG, READ_CONFIG_FILES)
    if USED_LOGGING_CONF is not None:
        LOGGER.debug("[%s] uses logging configuration %s", LOG_TAG, USED_LOGGING_CONF)
    LOGGER.info("[%s] manage record files from %s", LOG_TAG, RES_DIR)

    # evaluate configured server data, if any
    HOST = THE_CONFIG.get("server", "host", fallback="127.0.0.1")
    PORT = THE_CONFIG.getint("server", "port", fallback=8081)
    _RAW_IPS = THE_CONFIG.get("server", "accepted_ips", fallback="")
    CLIENT_IPS = [c.strip() for c in _RAW_IPS.split(",") if len(c.strip()) > 0]

    # forward to request handler
    START_DATA: HandlerInformation = HandlerInformation(RES_DIR, LOGGER)
    START_DATA.client_ips = CLIENT_IPS

    run_server(HOST, PORT, START_DATA)
