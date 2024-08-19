"""API processing and evaluating metadata in respect
to DDB (Deutsche Digitale Bibliothek = German Digital Library)
specific validations as well as XML schemata

Assuming DDB orders it's remarks in 5 categories:

1) info      ingested but information won't be used by DDB
2) warn      ingested without modification, but request for change
3) caution   ingested but suspicous
4) error     ingested after DDB did modifications
5) fatal     records won't be ingested at all

wiki.deutsche-digitale-bibliothek.de/display/DFD/Schematron-Validierungen+der+Fachstelle+Bibliothek
"""

import enum
import typing

from pathlib import Path

import lxml.etree as ET

import digiflow.common as dfc
import digiflow.validate as dfv
import digiflow.validate.metadata_xslt as df_vmdx


# add schematron validation language mapping
dfc.XMLNS['svrl'] = 'http://purl.oclc.org/dsdl/svrl'


# DDB Report information *not* to care about
# because we're using this for intermediate
# digitalization workflow results
IGNORE_DDB_RULES_INTERMEDIATE = [
    'fileSec_02',           # fatal: no mets:fileSec[@TYPE="DEFAULT"]

    # special cases for combined digital objects (i.e. volumes)
    'structMapLogical_17',  # fatal: metsPtr xlink:href invalid URN at import references SAF
    'structMapLogical_22',  # error: no fileGroup@USE='DEFAULT' at import not yet existing
    'fileSec_05',           # warn: no fileGroup FULLTEXT (newspapers)
]

IGNORE_DDB_RULES_ULB = IGNORE_DDB_RULES_INTERMEDIATE + [
    'originInfo_06',        # non-DDB-compliant mods:placeTerm
    'titleInfo_02',         # some titles are just shorter than 3 chars
    'structMapLogical_27',  # would not allow TYPE = cover_front
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


# please linter for lxml
# pylint: disable=c-extension-no-member


class DigiflowMetadataValidationException(Exception):
    """Mark Validation Criticals which
    otherwise prevent records from being
    imported by German Digital Library
    """


class DDBRole(enum.Enum):
    """Consider the severty of a meldung"""

    INFO = (1, 'info')
    WARN = (2, 'warn')
    CAUTION = (3, 'caution')
    ERROR = (4, 'error')
    FATAL = (5, 'fatal')

    def __init__(self, order: int, label: str):
        self.order = order
        self.label = label

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.order > other.order
        return NotImplemented

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.order < other.order
        return NotImplemented

    @classmethod
    def from_label(cls, label):
        """Get DDBRole object for label if exists"""
        if not isinstance(label, str):
            label = str(label)
        for role in DDBRole:
            if role.label.lower() == label.lower():
                return role
        return None


class DDBMeldung:
    """Individual DDB Meldung de-serialized from
    svrl:*-elements to gather @id, @role and 
    @location Attributes as well as optional 
    children svrl:text, svrl:description 
    and svrl:property
    """

    def __init__(self, elem: ET.Element):
        self._source = elem
        self.id: str = elem.get('id')
        self.role: DDBRole = DDBRole.from_label(elem.get('role'))
        self.location: str = elem.get('location')
        self.context: str = ''
        self._text = ''

    def __str__(self) -> str:
        return str(self.role.label, self.id)

    def explain(self):
        """Explain yourself"""
        msg_txt = f'[{self.id}] '
        properties = self._source.xpath('svrl:property', namespaces=dfc.XMLNS)
        if len(properties) > 0:
            _props = '=>'.join(f"{_p.get('id')}:{_p.text}" for _p in properties)
            msg_txt = f'{msg_txt} {_props}'
        if len(self.context) > 0:
            msg_txt = f'{msg_txt} test:{self.context}'
        descriptions = ' '.join(self._source.xpath('svrl:description/text()', namespaces=dfc.XMLNS))
        texts = '. '.join(self._source.xpath('svrl:text/text()', namespaces=dfc.XMLNS))
        explain_token = f'{descriptions} {texts}'
        if len(explain_token) > 1:  # because centered whitespace will exist
            msg_txt = f'{msg_txt} ({descriptions})'
        return msg_txt

    def add_context(self, ctx: str):
        """Add even more context on error data"""
        if ctx is not None and len(ctx) > 0:
            self.context = ctx

    def shortform(self) -> typing.Tuple:
        """Only basic information"""

        return (self.role.label, self.id)

    def longform(self) -> typing.Tuple:
        """All we know minus role"""

        return (self.id, self.explain())


class Report:
    """Report the state of object's
    metadata - includes collection of DDBMeldungen
    and optional schema error hints
    """

    def __init__(self, ddb_meldungen: typing.List[DDBMeldung]):
        self.ddb_meldungen = sorted(ddb_meldungen, key=lambda e: e.role.order, reverse=True)
        self.ignored_ddn_rules = []
        self.xsd_errors = None

    def read(self, only_headlines=True, map_ddb_roles=False) -> typing.List:
        """Get information from validation in order
        of their level from worse descending
        """

        if len(self.ddb_meldungen) == 0:
            return []
        delivery = self.ddb_meldungen
        if only_headlines:
            delivery = [m.shortform() for m in delivery]
        if map_ddb_roles:
            map_ddb_roles = {}
            for news in delivery:
                if isinstance(news, tuple):
                    the_key, appendum = news
                elif isinstance(news, DDBMeldung):
                    the_key = news.role.label
                    appendum = news.longform()
                if the_key not in map_ddb_roles:
                    map_ddb_roles.setdefault(the_key, [])
                map_ddb_roles[the_key].append(appendum)
            if len(map_ddb_roles) > 0:
                delivery = [f"{k}({len(v)}x):{v}" for k, v in map_ddb_roles.items()]
        return delivery

    def categorize(self, ignore_ddb_rule_ids, min_ddb_level):
        """Move meldungen to ignores if matching
        id labels (and optional below importance level
        threshold - to just ignore levels like 'info')
        """

        ignores = []
        respect = []
        for m in self.ddb_meldungen:
            if m.id in ignore_ddb_rule_ids:
                ignores.append(m)
            elif isinstance(min_ddb_level, str):
                current_role = m.role
                mininum_role = DDBRole.from_label(min_ddb_level)
                if (current_role and mininum_role) \
                        and current_role < mininum_role:
                    ignores.append(m)
                else:
                    respect.append(m)
            else:
                respect.append(m)
        self.ddb_meldungen = respect
        self.ignored_ddn_rules = ignores

    def alert(self, min_ddb_role_label=None):
        """Inspect current meldungen if something
        (per default) very severe lurks
        OR
        recognized schema errors
        """

        min_level = None
        if isinstance(min_ddb_role_label, str):
            min_level = DDBRole.from_label(min_ddb_role_label)
        if min_level is None:
            min_level = DDBRole.FATAL
        has_alerts = any(m for m in self.ddb_meldungen if m.role >= min_level)
        return has_alerts or self.xsd_errors is not None


class DDBTransformer:
    """Encapsulate technical aspects"""

    def __init__(self, path_input, path_xslt,
                 tmp_report_dir, tmp_report_file=None):
        self.path_input = path_input
        self.path_xslt = path_xslt
        self.tmp_report_dir = tmp_report_dir
        self.tmp_report_file = REPORT_FILE_XSLT
        if tmp_report_file is not None:
            self.tmp_report_file = tmp_report_file

    @property
    def report_path(self) -> Path:
        """Returns temporary report path"""
        if self.tmp_report_dir is None:
            self.tmp_report_dir = Path(self.path_input).parent
        return self.tmp_report_dir / self.tmp_report_file

    def transform(self):
        """Trigger actual report generation"""

        df_vmdx.transform(self.path_input, self.path_xslt, self.report_path)

    def clean(self):
        """Remove any artifacts if exist"""

        if self.report_path.exists():
            self.report_path.unlink()


class Reporter:
    """Encapsulates DDB conformant Validation
    of metadata with corresponding schema
    and the generation of DDBMeldungen for
    common PICA Types
    Switch for issues (PICA: AZ|OZ)
    """

    def __init__(self, path_input,
                 digi_type='Aa',
                 tmp_report_dir=None):
        self.path_input = path_input
        self.digi_type = digi_type
        self._report: typing.Optional[Report] = None
        path_xslt = PATH_MEDIA_XSL
        if self.digi_type[1] == 'Z':
            path_xslt = PATH_NEWSP_XSL
        self.transformer = DDBTransformer(path_input,
                                          path_xslt,
                                          tmp_report_dir)

    def get(self, ignore_ddb_rule_ids=None,
            min_ddb_level=None,
            validate_schema=True) -> Report:
        """get actual validation report with
        respect to custum ignore rule ids
        starting from provided minimal
        DDBRole level label (defaults to 'warn')
        """

        if self._report is None:
            self.transformer.transform()
            meldungen = self.extract_meldungen()
            ddb_report = Report(meldungen)
            self._report = ddb_report
            self.transformer.clean()
            if ignore_ddb_rule_ids is None:
                ignore_ddb_rule_ids = []
            if min_ddb_level is None:
                min_ddb_level = DDBRole.WARN.label
            ddb_report.categorize(ignore_ddb_rule_ids, min_ddb_level)
            if validate_schema:
                self.run_schema_validation()
        return self._report

    def extract_meldungen(self):
        """Information to gather details can be located in
        * svrl:failed-assert 
        * svrl:successful-report
        """

        tmp_root = ET.parse(self.transformer.report_path).getroot()
        sch_els = tmp_root.findall('svrl:*[@role]', dfc.XMLNS)
        if len(sch_els) > 0:
            meldungen = [DDBMeldung(e) for e in sch_els]
            self._enrich_location(meldungen)
            return meldungen
        return []

    def _enrich_location(self, meldungen):
        """Inspect results from tmp report file
        if any irregularities detected, apply XSLT2.0

        """
        try:
            path_input = self.path_input
            if not isinstance(path_input, str):
                path_input = str(self.path_input)
            for the_m in meldungen:
                if the_m.location is not None:
                    more_ctx = df_vmdx.evaluate(path_input, the_m.location)
                    if more_ctx.size > 0:
                        ctx_items = [more_ctx.item_at(i) for i in range(0, more_ctx.size)]
                        ctx = ','.join(_i.string_value.strip() for _i in ctx_items)
                        the_m.add_context(ctx)
        except Exception as _exc:
            raise DigiflowMetadataValidationException(_exc) from _exc

    def run_schema_validation(self):
        """Forward to XSD schema validation"""

        try:
            dfv.validate_xml(self.path_input,
                             xsd_mappings=dfv.METS_MODS_XSD)
        except dfv.InvalidXMLException as invalids:
            self._report.xsd_errors = invalids.args[0]

    def input_conform(self):
        """Check if any irregularities concerning
        XSD-schema recognized"""

        return self._report.xsd_errors is None
