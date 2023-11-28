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

from digiflow import (
    run_command
)

from .digiflow_metadata import (
    MetsReader,
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
    # but at migration time this is just a reference
    # to the local host SAF
    'structMapLogical_17',
]

# periodicals (end therefore periodical_volumes)
# may have lots of extended dateIssued elements
# which seem to confuse DDB Validation
DDB_IGNORE_RULES_NEWSPAPERS = [
    'originInfo_01',
]

# included validation schema files
DDB_MEDIA_SCH = 'ddb_validierung_mets-mods-ap-digitalisierte-medien.sch'
DDB_NEWSP_SCH = 'ddb_validierung_mets-mods-ap-digitalisierte-zeitungen.sch'
DDB_MEDIA_XSL = 'ddb_validierung_mets-mods-ap-digitalisierte-medien.xsl'
DDB_NEWSP_XSL = 'ddb_validierung_mets-mods-ap-digitalisierte-zeitungen.xsl'
SCH_DIR = Path(__file__).parent / 'resources' / 'sch'
XSL_DIR = Path(__file__).parent / 'resources' / 'xsl'
PATH_MEDIA_SCH = SCH_DIR / DDB_MEDIA_SCH
PATH_NEWSP_SCH = SCH_DIR / DDB_NEWSP_SCH
PATH_MEDIA_XSL = XSL_DIR / DDB_MEDIA_XSL
PATH_NEWSP_XSL = XSL_DIR / DDB_NEWSP_XSL

# default schematron cli jar path
# resides *outside* python package
# but still on project level
CLI_DIR = Path(__file__).parent.parent / 'digiflow' / 'schematron'
PATH_CLI = str(CLI_DIR / 'schxslt-cli.jar')

# temporary report files
REPORT_FILE_SCHEMATRON = 'report_schematron.xml'
REPORT_FILE_XSLT = 'resport_xslt.xml'

# which types require special treatment?
# if these show up, switch validation logics
DIGIS_MULTIVOLUME = ['Ac', 'Af', 'AF', 'Hc', 'Hf', 'HF',
                     'volume', 'periodical', 'periodical_volume']
DIGIS_NEWSPAPER = ['issue', 'additional', 'OZ', 'AZ']

# context text len
ASSERT_DSCR_MAX_LEN = 32
ASSERT_TEXT_MAX_LEN = 96
CRITICAL_VALIDATION_ROLES = ['critical', 'error', 'fatal']
DDB_ERROR = 'ddb_error'
DDB_OTHER = 'ddb_other'


class DigiflowDDBException(Exception):
    """Mark Validation Errors"""


class FailedAssert:
    """Encapsulate individual Schematron Report
    for failed assertion"""

    def __init__(self, elem: ET.Element):
        self.id = elem.get('id')
        self.role = elem.get('role')
        self.xpath = elem.get('location')
        self._text = '. '.join(elem.xpath('svrl:text/text()', namespaces=XMLNS))
        self._description = ' '.join(elem.xpath('svrl:description/text()', namespaces=XMLNS))
        self._properties = elem.xpath('svrl:property', namespaces=XMLNS)
        self.context = ''

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
            _txt = self._text.split('\n', maxsplit=1)[0]
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


def ddb_validation_sch(path_mets, path_schematron=PATH_MEDIA_SCH,
                       path_schematron_bin=PATH_CLI,
                       ignore_rules=None, aggregate_errors=True) -> Dict:
    """
    Apply DDB METS/MODS Validation from specific schematron template
    to given METS/MODS data
    * due subprocess forward might raise several exceptions
    * uses schxslt-cli.jar from https://github.com/schxslt/schxslt
    """

    if not os.path.isfile(path_mets):
        raise RuntimeError(f"missing {path_mets}")
    try:
        path_tmp_output = _forward_sch_cli(path_mets, path_schematron, path_schematron_bin)
        tmp_root = ET.parse(path_tmp_output).getroot()
        reports = tmp_root.findall('{http://purl.oclc.org/dsdl/svrl}*[@role]')
        n_reports = len(reports)
        if n_reports > 0:
            # curate ignore_rules only if required
            ignore_rules = _curate_ignorances(path_mets, ignore_rules)
            rois = [(e.attrib['role'], e.attrib['id'], e.attrib['location'])
                    for e in reports
                    if e.attrib['id'] not in ignore_rules]
            return _transform_report(rois, path_mets, aggregate_errors)
        # nothing bad to report
        return {}
    except Exception as exc:
        raise RuntimeError(exc.args) from exc


def _forward_sch_cli(path_mets, path_schematron, path_schematron_bin) -> str:
    path_mets_dir = os.path.dirname(path_mets)
    path_tmp_output = os.path.join(path_mets_dir, REPORT_FILE_SCHEMATRON)
    if not os.path.isfile(path_schematron):
        raise RuntimeError(f"missing {path_schematron}")
    if not os.path.isfile(path_schematron_bin):
        raise RuntimeError(f"missing {path_schematron_bin}")
    _cmd = f"""java -jar {path_schematron_bin} -s {path_schematron}\
                -d {path_mets} -o {path_tmp_output}"""
    run_command(_cmd, 30)
    return path_tmp_output


def _curate_ignorances(path_mets, ignore_rules) -> List[str]:
    # if valid List, no need to act
    if isinstance(ignore_rules, List):
        return ignore_rules
    # ... otherwise start to curate
    _rules = []
    # if raw string with commas, turn into List!
    if isinstance(ignore_rules, str):
        _rules = ignore_rules.split(',')
    # if unset, use proper ignore rules according to digi type!
    if ignore_rules is None:
        _rules = DDB_IGNORE_RULES_BASIC
        if _matches_digi_types(path_mets, DIGIS_MULTIVOLUME):
            _rules = DDB_IGNORE_RULES_MVW
    # return empty(=turned off) ignorance otherwise
    return _rules


def ddb_validation(path_mets, digi_type='Aa', ignore_rules=None):
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
    _path_xslt = str(PATH_MEDIA_XSL)
    if digi_type in DIGIS_NEWSPAPER:
        _path_xslt = str(PATH_NEWSP_XSL)
    if not isinstance(path_mets, str):
        path_mets = str(path_mets)
    # just create saxon processor context once
    # several calls seem to break stuff
    with PySaxonProcessor() as proc:
        _path_tmp_result_file = _validate_and_store_result(path_mets, proc, _path_xslt)
        if not os.path.isfile(_path_tmp_result_file):
            raise DigiflowDDBException(f"missing tmp output file {_path_tmp_result_file}")
        _failures = _get_failures(path_mets, proc, _path_tmp_result_file, ignore_rules)
        _aggregated = _aggregate_failures(_failures)
        if DDB_ERROR in _aggregated:
            raise DigiflowDDBException(_aggregated[DDB_ERROR])
        os.unlink(_path_tmp_result_file)
        return _aggregated


def _aggregate_failures(fails: List[FailedAssert]) -> Dict:
    _dict = {}
    for _f in fails:
        _xpln = _f.explain()
        _role = DDB_ERROR if _f.is_error() else DDB_OTHER
        if _role in _dict:
            _prev = _dict[_role]
            _prev.append(_xpln)
            _dict[_role] = _prev
        else:
            _dict.setdefault(_role, [_xpln])
    return _dict


def _matches_digi_types(path_mets, some_types):
    """determine if any of digi_types is of some_types
    by considering both PICA and logical types"""

    _types = list(MetsReader(path_mets).get_type_and_hierarchy()[:2])
    return any(t in some_types for t in _types)


def _validate_and_store_result(path_mets, proc, path_template):
    _path_tmp = None
    try:
        xsltproc = proc.new_xslt30_processor()
        _exec = xsltproc.compile_stylesheet(stylesheet_file=path_template)
        _doc_dir = os.path.dirname(path_mets)
        _path_tmp = os.path.join(_doc_dir, REPORT_FILE_XSLT)
        _doc_dir = os.path.dirname(path_mets)
        _path_tmp = os.path.join(_doc_dir, REPORT_FILE_XSLT)
        _exec.transform_to_file(source_file=path_mets,
                                stylesheet_file=path_template,
                                output_file=_path_tmp)
    except Exception as _exc:
        raise RuntimeError(_exc) from _exc
    return _path_tmp


def _get_failures(path_mets, proc, path_report, ignore_rules) -> List[FailedAssert]:
    """Inspect results from tmp report file
    if any irregularities detected, apply XSLT2.0
    expressions from report file to gather details"""

    _failures = []
    tmp_root = ET.parse(path_report).getroot()
    reports = tmp_root.findall('svrl:*[@role]', XMLNS)
    if len(reports) > 0:
        _fails = [FailedAssert(e)
                  for e in reports
                  if e.attrib['id'] not in ignore_rules]
        try:
            _mets_doc = proc.parse_xml(xml_file_name=path_mets)
            _xp_proc = proc.new_xpath_processor()
            _xp_proc.set_context(xdm_item=_mets_doc)
            for _fail in _fails:
                _info = _xp_proc.evaluate(_fail.xpath)
                if isinstance(_info, PyXdmValue) and _info.size > 0:
                    _items = [_info.item_at(i) for i in range(0, _info.size)]
                    _ctx = ','.join(_i.string_value.strip() for _i in _items)
                    _fail.add_context(_ctx)
                _failures.append(_fail)
        except Exception as _exc:
            raise RuntimeError(_exc) from _exc
    return _failures


def _transform_report(entries, path_mets, aggregate_errors):
    """Generate dictionary from schematron report data

    Args:
        entries (list): List of Schematron report entries
        path_mets (str): path to original METS/MODS

    Returns:
        dict: mapping role => list of entries for role
    """

    rois_map = {}
    mets_root = ET.parse(path_mets).getroot()
    for entry in entries:
        _xpath = _ddb_report_location_2_lxml_xpath(entry[2])
        affecteds = mets_root.xpath(_xpath, namespaces=XMLNS)
        _hits = _transform_affecteds(affecteds)
        hit_str = ','.join(_hits)
        if entry[0] in rois_map:
            rois_map[entry[0]].append((entry[1], hit_str))
        else:
            rois_map[entry[0]] = [(entry[1], hit_str)]
    # optional aggregate 'errors' and 'fatals'
    # merge category 'fatal' into 'error'
    if aggregate_errors:
        fatals = rois_map['fatal'] if 'fatal' in rois_map else []
        errors = rois_map['error'] if 'error' in rois_map else []
        total_errors = fatals + errors
        if total_errors and len(fatals) > 0:
            rois_map['error'] = total_errors
            del rois_map['fatal']
    return rois_map


def _transform_affecteds(affecteds):
    hits = []
    for aff in affecteds:
        if 'ID' in aff.attrib:
            hits.append(aff.attrib['ID'])
        elif 'type' in aff.attrib or 'source' in aff.attrib:
            attm = ','.join([(v + "=" + k)
                             for v, k in aff.attrib.items()])
            hits.append(attm)
        else:
            # get textual content only for *real*
            # subtrees not complete prime mods-tree
            # i.e. from mods:place
            _local_tagname = ET.QName(aff).localname
            _txt = ''
            if _local_tagname != 'mods':
                _txt = '='.join([t.strip() for t in aff.itertext()])
            hits.append(f"{__strip_namespaces(aff.tag)}{_txt}")
    return hits


def _ddb_report_location_2_lxml_xpath(xp_in):
    """Transform schematron output from DDB Report location data

    Args:
        xp_in (str): schematron failed-assert|success location attribute

    Returns:
        str: XPath stripped any qualified namespace annotations + leading '/'
    """

    return '/' + __strip_namespaces(xp_in)


def __strip_namespaces(elem_str):
    for _k, _v in XMLNS.items():
        if _v in elem_str:
            elem_str = elem_str.replace(_v, _k + ':')
    return elem_str.translate(str.maketrans({'Q': '', '{': '', '}': ''}))
