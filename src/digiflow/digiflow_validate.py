"""Helper to trigger DDB-Validation for digitized prints and newspaper issues/additionals
cf. https://github.com/Deutsche-Digitale-Bibliothek/ddb-metadata-schematron-validation
"""

import os

from typing import (
    Dict,
    List,
)

from pathlib import (
    Path
)

from lxml import etree as ET

from .digiflow_metadata import (
    XMLNS,
)

# trigger saxon API
SAXON_ENABLED = True
try:
    from saxonche import (
        PySaxonProcessor,
        PyXdmValue,
    )
except ModuleNotFoundError:
    SAXON_ENABLED = False

# add schematron validation language mapping
XMLNS['svrl'] = 'http://purl.oclc.org/dsdl/svrl'


# DDB Report information *not* to care about
# because we're using this for intermediate
# digitalization / migration results
DDB_IGNORE_RULES_BASIC = [
    'fileSec_02',       # fatal: no mets:fileSec[@TYPE="DEFAULT"]
    'identifier_01',    # info:  record identifier types like "bv" or "eki" not accepted
    'titleInfo_02',     # fatal: parts of work lack titel
]

# some special corner cases, when certain DDB-rules
# shall be ignored on this stage
# example:
# at migration time there's no valid href
# for the METS-pointer in the logical section, just
# the name of the SAF because Share_it creates this
# link afterwards beyond migration workflow
DDB_IGNORE_RULES_MVW = DDB_IGNORE_RULES_BASIC + [
    # fatal: metsPtr xlink:href must have valid URN
    # at import time it references local host SAF
    'structMapLogical_17',
    # error: no image from fileGroup@USE='DEFAULT' linked
    # at import time it's not yet existing
    'structMapLogical_22',
]

# periodicals (end therefore periodical_volumes)
# may have lots of extended dateIssued elements
# which seem to confuse DDB Validation
DDB_IGNORE_RULES_NEWSPAPERS = [
    'originInfo_01',
]

DDB_MEDIA_XSL = 'ddb_validierung_mets-mods-ap-digitalisierte-medien.xsl'
DDB_NEWSP_XSL = 'ddb_validierung_mets-mods-ap-digitalisierte-zeitungen.xsl'
XSL_DIR = Path(__file__).parent / 'resources' / 'xsl'
PATH_MEDIA_XSL = XSL_DIR / DDB_MEDIA_XSL
PATH_NEWSP_XSL = XSL_DIR / DDB_NEWSP_XSL

# default temporary report file
REPORT_FILE_XSLT = 'report_xslt.xml'

# which types require special treatment?
# if these show up, switch validation logics
DIGIS_MULTIVOLUME = ['Ac', 'Af', 'AF', 'Hc', 'Hf', 'HF',
                     'volume', 'periodical', 'periodical_volume']
DIGIS_NEWSPAPER = ['issue', 'additional', 'OZ', 'AZ']

# context text len
ASSERT_DSCR_MAX_LEN = 64
ASSERT_TEXT_MAX_LEN = 256
CRITICAL_VALIDATION_ROLES = ['critical', 'error', 'fatal']
FAILED_ASSERT_ERROR = 'failed_assert_error'
FAILED_ASSERT_OTHER = 'failed_assert_other'


class DigiflowTransformException(Exception):
    """Mark Missing Transformation Result"""


class DigiflowDDBException(Exception):
    """Mark Validation Errors"""


class FailedAssert:
    """Encapsulate individual Schematron Report
    Element svrl:failed-assert to gather
    @id, @role and @location Attributes
    as well as optional children svrl:text,
    svrl:description ans svrl:property
    """

    def __init__(self, elem: ET.Element):
        self.id: str = elem.get('id')
        self.role: str = elem.get('role')
        self.location: str = elem.get('location')
        self.text:str = elem.get('text')
        self.context: str = ''
        self._text: str = '. '.join(elem.xpath('svrl:text/text()', namespaces=XMLNS))
        self._description = ' '.join(elem.xpath('svrl:description/text()', namespaces=XMLNS))
        self._properties = elem.xpath('svrl:property', namespaces=XMLNS)

    def explain(self):
        """Explain the failure"""
        _msg = f'[{self.id}] '
        if len(self._properties) > 0:
            _props = '=>'.join(f"{_p.get('id')}:{_p.text}" for _p in self._properties)
            _msg = f'{_msg} {_props}'
        if len(self.context) > 0:
            _ctx = self.context
            if len(_ctx) > ASSERT_DSCR_MAX_LEN:
                _ctx = _ctx[:ASSERT_DSCR_MAX_LEN]
            _msg = f'{_msg} test:{_ctx}'
        _add_dscr = ''
        _add_text = ''
        if self._description:
            _dscr = self._description
            if len(_dscr) > ASSERT_DSCR_MAX_LEN:
                _dscr = _dscr[:ASSERT_DSCR_MAX_LEN]
            _add_dscr = _dscr + ', '
        if self._text:
            _txt = self._text
            if len(_txt) > ASSERT_TEXT_MAX_LEN:
                _txt = _txt[:(ASSERT_TEXT_MAX_LEN-3)] + '...'
            _add_text = _txt
        if len(_add_dscr) > 2 or len(_add_text) > 0:
            _msg = f'{_msg} ({_add_dscr}{_add_text})'
        return _msg

    def add_context(self, ctx: str):
        """Add even more context on error data"""
        if ctx is not None and len(ctx) > 0:
            self.context = ctx

    def is_error(self):
        """Need special care?"""
        return self.role in CRITICAL_VALIDATION_ROLES


def gather_failed_asserts(path_mets, processor, path_report, ignore_rules=None):
    """Gather all information about failed assertions"""

    _failures = _get_failures(path_mets, processor, path_report, ignore_rules)
    _aggregated = _failed_asserts_to_dict(_failures)
    if FAILED_ASSERT_ERROR in _aggregated:
        raise DigiflowDDBException(_aggregated[FAILED_ASSERT_ERROR])
    os.unlink(path_report)
    return _aggregated


def _get_failures(path_input, proc, path_report, ignores=None) -> List[FailedAssert]:
    """Inspect results from tmp report file
    if any irregularities detected, apply XSLT2.0
    expressions from report file to gather details"""

    _failures = []
    if ignores is None:
        ignores = []
    tmp_root = ET.parse(path_report).getroot()
    _failed_assert_roles = tmp_root.findall('svrl:*[@role]', XMLNS)
    if len(_failed_assert_roles) > 0:
        _fails = [FailedAssert(e)
                  for e in _failed_assert_roles
                  if e.attrib['id'] not in ignores]
        try:
            if not isinstance(path_input, str):
                path_input = str(path_input)
            _mets_doc = proc.parse_xml(xml_file_name=path_input)
            _xp_proc = proc.new_xpath_processor()
            _xp_proc.set_context(xdm_item=_mets_doc)
            for _fail in _fails:
                if _fail.location is not None:
                    _info = _xp_proc.evaluate(_fail.location)
                    if isinstance(_info, PyXdmValue) and _info.size > 0:
                        _items = [_info.item_at(i) for i in range(0, _info.size)]
                        _ctx = ','.join(_i.string_value.strip() for _i in _items)
                        _fail.add_context(_ctx)
                _failures.append(_fail)
        except Exception as _exc:
            raise RuntimeError(_exc) from _exc
    return _failures


def _failed_asserts_to_dict(fails: List[FailedAssert]) -> Dict:
    _dict = {}
    for _f in fails:
        _xpln = _f.explain()
        _role = FAILED_ASSERT_ERROR if _f.is_error() else FAILED_ASSERT_OTHER
        if _role in _dict:
            _prev = _dict[_role]
            _prev.append(_xpln)
            _dict[_role] = _prev
        else:
            _dict.setdefault(_role, [_xpln])
    return _dict


def ddb_validation(path_mets, digi_type='Aa', ignore_rules=None, post_process=gather_failed_asserts):
    """Process METS/MODS Validation which complies to Deutsche Digitale Bibliothek (DDB)
    plus applying additional set of rules which shall not be taken into account

    Further information can be found at:
        https://github.com/Deutsche-Digitale-Bibliothek/ddb-metadata-schematron-validation

    Args:
        path_mets (str|Path): resource for METS-file
        digi_type (str, optional): PICA-Type. Defaults to 'Aa'.
        ignore_rules (_type_, optional): Which Rules shall be
            ignored when evaluating validation outcome.
            Defaults to DDB_IGNORE_RULES_BASIC.
    """
    if ignore_rules is None:
        ignore_rules = DDB_IGNORE_RULES_BASIC
    if digi_type in DIGIS_MULTIVOLUME:
        ignore_rules = DDB_IGNORE_RULES_MVW
    _path_xslt = str(PATH_MEDIA_XSL)
    if digi_type in DIGIS_NEWSPAPER:
        _path_xslt = str(PATH_NEWSP_XSL)
    if not isinstance(path_mets, str):
        path_mets = str(path_mets)
    # just create saxon processor context once
    # several calls seem to break stuff
    with PySaxonProcessor() as proc:
        _path_result_file = _transform_to(path_mets, proc, _path_xslt)
        if not os.path.isfile(_path_result_file):
            raise DigiflowDDBException(f"missing tmp output file {_path_result_file}")
        return post_process(path_mets, proc, _path_result_file, ignore_rules)


def apply(path_input_xml, path_xslt, path_result_file=None, post_process=None) -> Path:
    """Low-level API to apply XSLT-Transformation
    like Schematron Validation Report Language XSLT
    on given input file and store outcome in local
    result file"""

    with PySaxonProcessor() as proc:
        _path_result_file = _transform_to(
            path_input_xml, proc, path_xslt, path_result_file)
        if not os.path.isfile(_path_result_file):
            raise DigiflowTransformException(f"missing result {_path_result_file}")
        if post_process is not None:
            return post_process(path_input_xml, proc, _path_result_file)
        return _path_result_file


def _transform_to(path_input_file, proc, path_template, path_result_file=None):
    if not isinstance(path_input_file, str):
        path_input_file = str(path_input_file)
    if not isinstance(path_template, str):
        path_template = str(path_template)
    if path_result_file is None:
        _the_dir = os.path.dirname(path_input_file)
        path_result_file = os.path.join(_the_dir, REPORT_FILE_XSLT)
    if not isinstance(path_result_file, str):
        path_result_file = str(path_result_file)
    try:
        xsltproc = proc.new_xslt30_processor()
        _exec = xsltproc.compile_stylesheet(stylesheet_file=path_template)
        _exec.transform_to_file(source_file=path_input_file,
                                output_file=path_result_file)
    except Exception as _exc:
        raise DigiflowTransformException(_exc) from _exc
    return Path(path_result_file).resolve()
