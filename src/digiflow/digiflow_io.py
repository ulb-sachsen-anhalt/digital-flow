"""I/O related helpers"""

import ast
import email.utils
import email.mime.text
import os
import shutil
import smtplib

from pathlib import Path

import requests
from lxml import etree as ET

import digiflow.common as dfc
import digiflow.digiflow_metadata as df_md

# please pylinter
# pylint:disable=c-extension-no-member

OAI_KWARG_FGROUP_IMG = "fgroup_images"
DEFAULT_FGROUP_IMG = "MAX"
OAI_KWARG_FGROUP_OCR = "fgroup_ocr"
DEFAULT_FGROUP_OCR = "FULLTEXT"
OAI_KWARG_POSTFUNC = "post_oai"
OAI_KWARG_REQUESTS = "request_kwargs"
REQUESTS_DEFAULT_TIMEOUT = 20


class LoadException(Exception):
    """Load of OAI Data failed"""


class ServerError(LoadException):
    """Loading of Record via OAI failed due
    response status_code indicating Server Error"""


class ClientError(LoadException):
    """Loading of Record via OAI failed due 
    response status_code indicating Client Error"""


class ContentException(LoadException):
    """Loading of Record via OAI failed due unexpected 
    returned content, indicating missing record data 
    or even missing complete record"""


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
        self.key_images = kwargs[OAI_KWARG_FGROUP_IMG] \
            if OAI_KWARG_FGROUP_IMG in kwargs else DEFAULT_FGROUP_IMG
        self.key_ocr = kwargs[OAI_KWARG_FGROUP_OCR] \
            if OAI_KWARG_FGROUP_OCR in kwargs else DEFAULT_FGROUP_OCR
        self.post_oai = kwargs[OAI_KWARG_POSTFUNC] \
            if OAI_KWARG_POSTFUNC in kwargs else None
        self.groups[self.key_images] = []
        self.request_kwargs = self._sanitize_kwargs(kwargs)
        self.groups[self.key_ocr] = []
        self.store = None

    def _sanitize_kwargs(self, in_kwargs):
        top_dict = {}
        if OAI_KWARG_REQUESTS in in_kwargs:
            raw_kwargs = in_kwargs[OAI_KWARG_REQUESTS]
            if isinstance(raw_kwargs, dict):
                return raw_kwargs
            top_tokens = [r.strip() for r in raw_kwargs.split(",")]
            for t in top_tokens:
                k, v = t.split("=", maxsplit=1)
                try:
                    top_dict[k] = ast.literal_eval(v)
                except ValueError as val_err:
                    msg = f"Can't use {top_dict} to load: {val_err.args[0]}"
                    raise LoadException(msg) from val_err
        return top_dict

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
        ctx = f"verb=GetRecord&metadataPrefix={metadata_format}&identifier={record_identifier}"
        res_url = f"{self.base_url}?{ctx}"
        self.path_mets = local_dst
        path_res = self._handle_load(res_url, self.path_mets, self.post_oai, force_update)
        if path_res:
            loaded += 1

        if skip_resources:
            return loaded

        # inspect if additional file resources are requested
        mets_reader = df_md.MetsReader(self.path_mets, mets_digital_object_identifier)

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
                if dfc.XMLNS['mets'] in _snippet or dfc.XMLNS['oai'] in _snippet:
                    data = post_func(self.path_mets, data)
                elif 'http://www.loc.gov/standards/alto' in _snippet:
                    data = post_func(local_path, data)
                else:
                    raise LoadException(f"Can't handle {content_type} from {url}!")
            return local_path
        except LoadException as load_exc:
            raise load_exc
        except Exception as exc:
            msg = f"load {url} exception: {exc}"
            raise RuntimeError(msg) from exc


class OAIFileSweeper:
    """ delete all resources (files and empty folder),
        except colorchecker, if any
        parse *.mets.xml to identify files to be deleted
    """

    def __init__(self, path_store, pattern='mets.xml', filegroups=None):
        self.work_dir = path_store
        self.pattern = pattern
        self.filegroups = filegroups if isinstance(filegroups, list)\
            else ["MAX"]

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
                                return f"cannot delete {pth} due insuff. perm."
        return (work_dir, total, f"{(size >> 20)} Mb")

    def _get_files(self, mets_xml, filegroup):
        xml_root = ET.parse(str(mets_xml)).getroot()
        xpath = f".//mets:fileGrp[@USE='{filegroup}']/mets:file/mets:FLocat"
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
        the_timeout = REQUESTS_DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            the_timeout = kwargs["timeout"]
            del kwargs["timeout"]
        response = requests.get(url, timeout=the_timeout, **kwargs)
        status = response.status_code
        if status >= 400:
            the_info = f"{url} status {status}"
            if status < 500:
                raise ClientError(the_info)
            raise ServerError(the_info)
        if status == 200:
            content_type = response.headers['Content-Type']
            # textual xml data
            if 'text' in content_type or 'xml' in content_type:
                result = response.content
                xml_root = ET.fromstring(result)
                check_error = xml_root.find('.//error', xml_root.nsmap)
                if check_error is not None:
                    msg = f"request {url} failed due {check_error.text}"
                    raise LoadException(msg)
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
                msg = f"download {url} with unhandled content-type {content_type}"
                raise ContentException(msg)
        return (path_local, result, content_type)
    except (OSError) as exc:
        msg = f"fail to download {url} to {path_local}"
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


def smtp_note(smtp_conn: str, subject: str, message: str, froms: str, tos):
    """Notify recipients about subject
    in true local time
    stmp_connn: str Information about host:port
    subject: str The Subject
    message: str The Message
    sender: str Email of sender
    recipients: str, list  Email(s)
    """

    if isinstance(tos, list):
        tos = ','.join(tos)
    try:
        msg = email.mime.text.MIMEText(message)
        msg['Date'] = email.utils.formatdate(localtime=True)
        msg['Subject'] = subject
        msg['From'] = froms
        msg['To'] = tos + "\n"
        server = smtplib.SMTP(smtp_conn)
        server.send_message(msg)
        server.quit()
        return f"'{tos}' note: '{message}'"
    except Exception as _exc:
        return f"Failed to notify '{tos}': '{_exc.args[0]}' ('{message}')!"


def post_oai_store_ocr(path_local, the_data):
    """
    Store OCR XML as it is
    Explicite encoding is required with OCR-strings but not
    for byte objects like from semantics fulltext responses
    """

    if isinstance(the_data, str):
        the_data = the_data.encode('utf-8')
    xml_root = ET.fromstring(the_data)
    df_md.write_xml_file(xml_root, path_local, preamble=None)


def get_enclosed(tokens_str: str, mark_end='}', mark_start='{', func_find='rfind') -> str:
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
