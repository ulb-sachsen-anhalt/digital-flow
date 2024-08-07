"""API processing and evaluating DDB validations

Assume DDB (Deutsche Digitale Bibliothek = German Digital Library)
order it's remarks into 5 categories:

* info      ingested but some information won't be used by DDB
* warn      error but ingested without modification
* caution   ingest but suspicous
* error     ingest after DDB-modification
* fatal     records can't be ingested => strict mode throws exception

wiki.deutsche-digitale-bibliothek.de/display/DFD/Schematron-Validierungen+der+Fachstelle+Bibliothek
"""

import os
import typing

from pathlib import Path

import lxml.etree as ET

import digiflow.common as dfc

from digiflow.validate.metadata_xslt import transform, evaluate

# add schematron validation language mapping
dfc.XMLNS['svrl'] = 'http://purl.oclc.org/dsdl/svrl'


# DDB Report information *not* to care about
# because we're using this for intermediate
# digitalization / migration results
DDB_IGNORE_RULES_BASIC = [
    'fileSec_02',       # fatal: no mets:fileSec[@TYPE="DEFAULT"]
    'identifier_01',    # info:  record identifier types like "bv" or "eki" not accepted
    'titleInfo_02',     # fatal: parts of work lack titel
    # 'originInfo_06',  # error: unclear = placeTerm contains invalid attr type 'code' (i.e. XA-DE)
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

_DDB_MEDIA_XSL = 'ddb_validierung_mets-mods-ap-digitalisierte-medien.xsl'
_DDB_NEWSP_XSL = 'ddb_validierung_mets-mods-ap-digitalisierte-zeitungen.xsl'
_XSL_DIR = Path(__file__).parent.parent / 'resources' / 'xsl'
PATH_MEDIA_XSL = _XSL_DIR / _DDB_MEDIA_XSL
PATH_NEWSP_XSL = _XSL_DIR / _DDB_NEWSP_XSL

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


class DigiflowDDBException(Exception):
    """Mark Validation Criticals which
    otherwise prevent records from being
    imported by German Digital Library
    """


class DDBMeldung:
    """Individual DDB Meldung de-serialized from
    svrl:*-elements to gather @id, @role and 
    @location Attributes as well as optional 
    children svrl:text, svrl:description 
    and svrl:property
    """

    def __init__(self, elem: ET.Element):
        self.id: str = elem.get('id')
        self.role: str = elem.get('role')
        self.location: str = elem.get('location')
        self.text:str = elem.get('text')
        self.context: str = ''
        self._text: str = '. '.join(elem.xpath('svrl:text/text()', namespaces=dfc.XMLNS))
        self._description = ' '.join(elem.xpath('svrl:description/text()', namespaces=dfc.XMLNS))
        self._properties = elem.xpath('svrl:property', namespaces=dfc.XMLNS)

    def explain(self):
        """Explain yourself"""
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


class DDBReporter:
    """Encapsulates DDB conformant Validation
    of metadata with corresponding schema
    and the generation of DDBMeldungen
    """

    def __init__(self, path_input,
                 digi_type='Aa', ignore_ids=None,
                 tmp_report_dir=None):
        self.path_input = path_input
        self._meldungen = []
        self.ignore_ids = []
        if isinstance(ignore_ids, list) and len(ignore_ids) > 0:
            self.ignore_ids = ignore_ids
        self.digi_type = digi_type
        self.path_xslt = PATH_MEDIA_XSL
        if self.digi_type[1] == 'Z':
            self.path_xslt = PATH_NEWSP_XSL
        self.tmp_report_dir = tmp_report_dir
        self.tmp_report_file = REPORT_FILE_XSLT

    @property
    def report_path(self) -> Path:
        """Returns temporary report path"""
        if self.tmp_report_dir is None:
            self.tmp_report_dir = Path(self.path_input).parent
        return self.tmp_report_dir / self.tmp_report_file
    
    @property
    def meldungen(self) -> typing.List[DDBMeldung]:
        """get actual validation"""

        if len(self._meldungen) == 0:
            self._meldungen = transform(self.path_input, self.path_xslt, self.report_path)
            self._enrich_location()
        return self._meldungen

    def _enrich_location(self):
        """Inspect results from tmp report file
        if any irregularities detected, apply XSLT2.0

        """
        self._meldungen = self._as_meldungen()
        if len(self._meldungen) == 0:
            return self._meldungen
        try:
            path_input = self.path_input
            if not isinstance(path_input, str):
                path_input = str(self.path_input)
            for the_m in self.meldungen:
                if the_m.location is not None:
                    more_ctx = evaluate(path_input, the_m.location)
                    if more_ctx.size > 0:
                        ctx_items = [more_ctx.item_at(i) for i in range(0, more_ctx.size)]
                        ctx = ','.join(_i.string_value.strip() for _i in ctx_items)
                        the_m.add_context(ctx)
        except Exception as _exc:
            raise RuntimeError(_exc) from _exc

    def _as_meldungen(self):
        """Information to gather details can be located in
        * svrl:failed-assert 
        * svrl:successful-report
        """

        tmp_root = ET.parse(self.report_path).getroot()
        sch_els = tmp_root.findall('svrl:*[@role]', dfc.XMLNS)
        if len(sch_els) > 0:
            return [DDBMeldung(e)
                    for e in sch_els
                    if e.attrib['id'] not in self.ignore_ids]
        return []

    def clean(self):
        """Remove any artifacts if exist"""

        if self.report_path.exists():
            self.report_path.unlink()


def gather_failed_asserts(path_mets, path_report, ignore_rules=None):
    """Gather all information about failed assertions"""

    _failures = _get_failures(path_mets, path_report, ignore_rules)
    _aggregated = _failed_asserts_to_dict(_failures)
    if FAILED_ASSERT_ERROR in _aggregated:
        raise DigiflowDDBException(_aggregated[FAILED_ASSERT_ERROR])
    os.unlink(path_report)
    return _aggregated


def _get_failures(path_input, proc, path_report, ignores=None) -> typing.List[DDBMeldung]:
    """Inspect results from tmp report file
    if any irregularities detected, apply XSLT2.0
    expressions from report file to gather details"""

    _failures = []
    if ignores is None:
        ignores = []
    tmp_root = ET.parse(path_report).getroot()
    _failed_assert_roles = tmp_root.findall('svrl:*[@role]', dfc.XMLNS)
    if len(_failed_assert_roles) > 0:
        _fails = [DDBMeldung(e)
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


def _failed_asserts_to_dict(fails: typing.List[DDBMeldung]) -> typing.Dict:
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


def ddb_validation(path_mets, digi_type='Aa', ignore_rules=None, 
                   post_process=gather_failed_asserts, path_result_file=None):
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
    if path_result_file is None:
        _the_dir = os.path.dirname(path_mets)
        path_result_file = os.path.join(_the_dir, REPORT_FILE_XSLT)
    _path_result_file = transform(path_mets, _path_xslt, path_result_file)
    if not os.path.isfile(_path_result_file):
        raise DigiflowDDBException(f"missing tmp output file {_path_result_file}")
    return post_process(path_mets, _path_result_file, ignore_rules)
    

