"""Metadata module regarding METS/MODS"""

# -*- coding: utf-8 -*-

import abc
import dataclasses
import os
import time
import typing

from collections import (
    defaultdict
)
from configparser import (
    ConfigParser
)
from pathlib import (
    Path
)
from typing import (
    List,
    Optional,
    Tuple
)

from lxml import etree as ET

import digiflow.common as dfc


# due lxml
# pylint:disable=c-extension-no-member

# old-school: register all namespaces known so-far for writing
ET.register_namespace('dc', 'http://purl.org/dc/elements/1.1/')
ET.register_namespace('dv', 'http://dfg-viewer.de/')
ET.register_namespace('epicur', 'urn:nbn:de:1111-2004033116')
ET.register_namespace('marcxml', 'http://www.loc.gov/MARC21/slim')
ET.register_namespace('mets', 'http://www.loc.gov/METS/')
ET.register_namespace('mods', 'http://www.loc.gov/mods/v3')
ET.register_namespace('oai', 'http://www.openarchives.org/OAI/2.0/')
ET.register_namespace('oai_dc', 'http://www.openarchives.org/OAI/2.0/oai_dc/')
ET.register_namespace('ulb', 'https://bibliothek.uni-halle.de')
ET.register_namespace('vl', 'http://visuallibrary.net/vl')
ET.register_namespace('vls', 'http://semantics.de/vls')
ET.register_namespace('vlz', 'http://visuallibrary.net/vlz/1.0/')
ET.register_namespace('xlink', 'http://www.w3.org/1999/xlink')
ET.register_namespace('zvdd', 'http://zvdd.gdz-cms.de/')

#
# METS/MODS common python-flavour XML expressions / namespaces attributes
#
XLINK_HREF = '{http://www.w3.org/1999/xlink}href'
XLINK_FROM = '{http://www.w3.org/1999/xlink}from'
XLINK_TO = '{http://www.w3.org/1999/xlink}to'
XPATH_STRUCTMAP_LOG = './/mets:structMap[@TYPE="LOGICAL"]/mets:div'
XPATH_MODS_VL_PICA = './/mods:extension/vlz:externalType[@recordSyntax="pica"]'
MARK_KITODO2 = 'kitodo2'
MARK_KITODO3 = 'kitodo3'
MARK_VLS = 'vls'
MARK_AGENT_LEGACY = 'legacy vlid: '
MARK_AGENT_VLID = 'created vlid: '
XPR_MODS_SEC = './/mets:dmdSec/mets:mdWrap/mets:xmlData/mods:mods/../../..'
PATTERN_FILEGROUP_USE = './/mets:fileGrp[@USE="{}"]'

# script constants
DEFAULT_FLAT_STRUCTS = [
    'monograph', 'manuscript'
]
DEFAULT_PARENT_STRUCTS = [
    'multivolume', 'multivolume_work', 'multivolumework',
    'fulltome', 'newspaper', 'year', 'periodical'
]
CONTENT_FILE_GROUPS = [
    'MAX', 'LOCAL'
]


def write_xml_file(xml_root,
                   outfile,
                   preamble='<?xml version="1.0" encoding="UTF-8"?>'):
    """write xml root prettified to outfile
    with default preamble for file usage
    create export dir if not exists and writeable
    """

    _prettified = _pretty_xml(xml_root, preamble)
    dst_dir = os.path.dirname(outfile)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    with open(outfile, 'wb') as file_handler:
        file_handler.write(_prettified)


def _pretty_xml(xml_root, preamble='<?xml version="1.0" encoding="UTF-8"?>'):
    """XML root as prettified string
    witd cleared namespace declarations
    and default preamble like for XML files

    disable preamble by setting it to 'None'
    """
    _as_string = ET.tostring(ET.ElementTree(xml_root), pretty_print=True, encoding='UTF-8')
    _pretty_parser = ET.XMLParser(resolve_entities=False,
                                  strip_cdata=False,
                                  remove_blank_text=True)
    _root = ET.fromstring(_as_string, _pretty_parser)
    ET.cleanup_namespaces(_root, top_nsmap=dfc.XMLNS)
    _formatted = ET.tostring(_root, pretty_print=True, encoding='UTF-8').decode('UTF-8')
    if preamble:
        formatted_file_content = f'{preamble}\n{_formatted}'
    else:
        formatted_file_content = f'{_formatted}'
    return formatted_file_content.encode('UTF-8').replace(b'\n', b'\r\n')


def _post_oai_extract_metsdata(xml_tree):
    """Extract METS as new root from OAI envelope"""

    namespace = xml_tree.xpath('namespace-uri(.)')
    if namespace == 'http://www.loc.gov/METS/':
        return xml_tree
    if namespace == 'http://www.openarchives.org/OAI/2.0/':
        mets_root_el = xml_tree.find('.//mets:mets', dfc.XMLNS)
        if mets_root_el is not None:
            return ET.ElementTree(mets_root_el).getroot()
    return None


def extract_mets(path_mets, the_data):
    """Just extract METS from OAI body"""

    xml_root = ET.fromstring(the_data)
    mets_tree = _post_oai_extract_metsdata(xml_root)
    write_xml_file(mets_tree, path_mets, preamble=None)


@dataclasses.dataclass
class METSFileInfo:
    """Represents filGrp/file entry"""
    file_id: str
    file_type: str
    loc_url: str


class XMLProcessor(abc.ABC):
    """Basic XML-Processing"""

    def __init__(self, path_xml, xmlns=None):
        self.tree = None
        if not os.path.exists(path_xml):
            raise RuntimeError(f"{path_xml} not existing!")
        self.path_xml = path_xml
        self.xmlns = xmlns
        if not self.xmlns:
            self.xmlns = dfc.XMLNS
        self.path_xml_dir = os.path.dirname(path_xml)
        self._parse()

    def _parse(self):
        self.tree = ET.parse(self.path_xml).getroot()

    def remove(self, tags):
        """remove elements by tagname"""

        for tag in tags:
            removals = self.tree.findall(f'.//{tag}', self.xmlns)
            for rem in removals:
                parent = rem.getparent()
                parent.remove(rem)
                # also clear parent textual hidden linebreaks
                if len(parent.getchildren()) == 0 and parent.text:
                    parent.text = ''

    def findall(self, expression, element=None, get_all=True):
        """wrap search by xpath-expression"""
        els = None
        if element is not None:
            els = element.findall(expression, dfc.XMLNS)
        else:
            els = self.tree.findall(expression, dfc.XMLNS)
        if els is None:
            return []
        if not get_all:
            return els[0]
        return els

    def xpath(self, expression, element=None):
        """wrap xpath 1.0 calls"""
        if element is not None:
            return element.xpath(expression, namespaces=dfc.XMLNS)
        return self.tree.xpath(expression, namespaces=dfc.XMLNS)

    def write(self, ext='.xml', out_dir=None, suffix=None) -> str:
        """Write XML-Data to METS/MODS with optional suffix"""

        file_name = self.path_xml

        # if suffix was set, trigger name creation logic below
        if suffix:
            file_name = os.path.splitext(os.path.basename(self.path_xml))[0]
            if "." in file_name:
                file_name = file_name.split(".")[0]
            if suffix:
                file_name += '.' + suffix + ext
            else:
                file_name += ext

        mets_dir = self.path_xml_dir
        if out_dir:
            if not os.path.exists(out_dir):
                raise RuntimeError(f'invalid destination {out_dir}')
            mets_dir = out_dir

        path_out = os.path.join(mets_dir, file_name)
        write_xml_file(self.tree, path_out)
        return path_out


class MetsProcessor(XMLProcessor):
    """Basic METS/MODS-Handling"""

    def enrich_agent(self, agent_name, agent_note=None, **kwargs):
        """Enrich agent information by name
        at the proper Header position
        optional set note/remark (per default current date)
        and additional mets:agent attributes as kwargs => 
        be sure to know how to do, since the schema is
        rigorous concerning *both* attributes and values!
        """

        mets_hdr: ET._Element = self.tree.find('.//mets:metsHdr', dfc.XMLNS)
        next_agent_id = 0
        for i, sub_el in enumerate(mets_hdr.getchildren(), 1):
            if ET.QName(sub_el.tag).localname == 'agent':
                next_agent_id = i
        if kwargs is None or len(kwargs) == 0:
            kwargs = {'TYPE': 'OTHER', 'ROLE': 'OTHER', 'OTHERTYPE': 'SOFTWARE'}
        agent_smith = ET.Element('{http://www.loc.gov/METS/}agent', kwargs)
        the_name = ET.SubElement(agent_smith, '{http://www.loc.gov/METS/}name')
        the_name.text = agent_name
        the_note = ET.SubElement(agent_smith, '{http://www.loc.gov/METS/}note')
        if agent_note is None:
            agent_note = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        the_note.text = agent_note
        mets_hdr.insert(next_agent_id, agent_smith)

    def clear_agents(self, filter_by_attr, black_list):
        """
        Drop agent entries in metsHdr section
        - usefull to drop information from intermediate OCR-Steps or Legacy tools
        - can be filtered by node@attribute with attribute blacklist
        """

        agents = self.findall('.//mets:metsHdr/mets:agent')
        for agent in agents:
            if filter_by_attr in agent.keys():
                filter_attr = agent.attrib[filter_by_attr]
                if filter_attr:
                    for black in black_list:
                        if black in filter_attr:
                            parent = agent.getparent()
                            parent.remove(agent)

    def contains_group(self, group: 'str|List[str]') -> bool:
        """Test if a certain fileGroup exists"""

        if isinstance(group, list):
            return any(self.tree.findall(PATTERN_FILEGROUP_USE.format(g), dfc.XMLNS)
                       for g in group)
        _fgroups = self.tree.findall(PATTERN_FILEGROUP_USE.format(group), dfc.XMLNS)
        return len(_fgroups) > 0

    def clear_filegroups(self, black_list=None):
        """Clear file Groups by blacklist"""

        cleared = 0
        file_sections = self.tree.findall('.//mets:fileSec', dfc.XMLNS)
        if len(file_sections) < 1:
            return cleared

        sub_groups = list(file_sections[0])
        for sub_group in sub_groups:
            subgroup_label = sub_group.attrib['USE']
            if black_list:
                if subgroup_label in black_list:
                    self._clear_also_fileptrs(sub_group)
                    file_sections[0].remove(sub_group)
                    cleared += 1

        # optional: handle *now* possibly empty file section
        if len(file_sections[0].getchildren()) < 1:
            parent = file_sections[0].getparent()
            parent.remove(file_sections[0])
        return cleared

    def _clear_also_fileptrs(self, sub_group):
        """
        Drop outdated file pointer references (mets:fptr)
        not only in physical Sequence Map but also in logical structMap
        """

        files = sub_group.findall('mets:file', dfc.XMLNS)
        for _file in files:
            file_id = _file.attrib['ID']
            # self._sanitze_reference_and_page(file_id)
            references = self.tree.findall(
                f'.//mets:fptr[@FILEID="{file_id}"]', dfc.XMLNS)
            for reference in references:
                parent = reference.getparent()
                parent.remove(reference)


class MetsReaderReport:
    """Wrapper for digital object data analysis outcome"""

    def __init__(self):
        self.identifiers = {}
        self.languages = []
        self.system_identifiers = {}
        self.type = None
        self.hierarchy = None
        self.links = None
        self.images = None
        self.locations = None
        self.origins = None


PREFIX_VLS = 'md'

NEWSPAPER_TYPES = ['issue', 'additional', 'year', 'newspaper']


class MetsReader(MetsProcessor):
    """
    Read and try to make sense of METS/MODS Metadata which
    might be enclosed in OAI-PMH or as local file.

    Each METS-Record is considere to have at least
    * a primary descriptive Metadata section (prime_dmd), and 
    * a structure Mapping, optionally marked with @TYPE=LOGICAL

    For Identifiers of primary descriptive Metadata sections (prime_dmd) holds
    * VLS-Records are prefixed 'md' and the ID is considered numeric
    * Kitodo2 Monographs are fixed to "DMDLOG_0000"
    * Kitodo2 MVW are paired "DMDLOG_0000" - "DMDLOG_0001" 
    * DSpace item handle based record contain a single '/' 
    * Kitodo3 objects use UUIDs
    """

    def __init__(self, path_mets, dmd_id: str = None):
        super().__init__(path_mets)
        self._prime_type: Optional[str] = None
        self._prime_mods = None
        self._prime_mods_id: Optional[str] = dmd_id
        if self._prime_mods_id is None:
            self._determine_prime_dmd_id()
        self._determine_prime_dmd()
        self._report: Optional[MetsReaderReport] = None
        self._config = None

    @property
    def config(self):
        """Access configuration"""
        return self._config

    @config.setter
    def config(self, path_config: Path):
        """Determine configuration"""
        if not path_config.is_file():
            raise RuntimeError(f"invalid path config '{path_config}'!")
        _cp = ConfigParser()
        _cp.read(path_config)
        self._config = _cp

    def _determine_prime_dmd_id(self):
        """
        Inspect METS
        * if only single MODS present, take it
        * otherwise, guess prime by structure map
        """

        dmd_candidates = self.tree.findall(XPR_MODS_SEC, dfc.XMLNS)
        if len(dmd_candidates) == 1 and 'ID' in dmd_candidates[0].attrib:
            self._prime_mods_id = dmd_candidates[0].attrib['ID']
        else:
            # if more MODS present
            # we are only interested in those with identifiers present
            primes = [d.get('ID')
                      for d in dmd_candidates
                      if len([m for m in d.iterdescendants() if 'identifier' in m.tag]) > 0
                      ]
            if len(primes) == 1:
                self._prime_mods_id = primes[0]
            else:
                self._prime_mods_id = self._determine_prime_dmd_id_by_logical_struct()

    def _determine_prime_dmd_id_by_logical_struct(self) -> str:
        log_struct = self.tree.find('.//mets:structMap[@TYPE="LOGICAL"]', dfc.XMLNS)
        if log_struct is None:
            raise RuntimeError(f"No logical struct in {self.tree.base}!")
        first_level = log_struct.getchildren()
        _raw_id = None
        if len(first_level) == 1:
            the_type: str = first_level[0].attrib['TYPE'].lower()
            if the_type in DEFAULT_FLAT_STRUCTS:
                _raw_id = first_level[0].attrib['DMDID']
            elif the_type in DEFAULT_PARENT_STRUCTS:
                # parent structs are considered not to have fileGroup for MAX images
                # if not self.tree.findall('.//mets:fileGrp[@USE="MAX"]', XMLNS):
                if not self.contains_group(CONTENT_FILE_GROUPS):
                    _raw_id = first_level[0].attrib['DMDID']
                else:
                    # a subsequent digital object (monograph, volume, issue) should have MAX images
                    # and further, it should posses a physical root mapping
                    _xpr_root_link = f'.//mets:structLink/mets:smLink[@{XLINK_TO}="physroot"]'
                    _root_link = self.tree.find(_xpr_root_link, dfc.XMLNS)
                    if _root_link is not None:
                        _log_id = _root_link.attrib[f'{{{dfc.XMLNS["xlink"]}}}from']
                    # no physical root link found
                    # now map most frequent xlink:from reference
                    # this must be the logical root
                    else:
                        _xpr_link = './/mets:structLink/mets:smLink'
                        _links = self.tree.findall(_xpr_link, dfc.XMLNS)
                        _link_map = defaultdict(int)
                        for _link in _links:
                            _from = _link.attrib[f'{{{dfc.XMLNS["xlink"]}}}from']
                            _link_map[_from] += 1
                        # now sort entries
                        # get first match which must be the "top-linker"
                        _log_id = sorted(_link_map, key=lambda e: e[1])[0]
                    # ... so
                    # we shall know by now the top log id
                    _xpr_log = f'.//mets:structMap[@TYPE="LOGICAL"]//mets:div[@ID="{_log_id}"]'
                    _log = self.tree.find(_xpr_log, dfc.XMLNS)
                    if _log is not None:
                        _raw_id = _log.attrib['DMDID']
        # if still no primary id found, go nuts
        if not _raw_id:
            raise RuntimeError(f"Can't find primary dmd_id in {self.tree.base}")
        return _raw_id

    def _determine_prime_dmd(self):
        """Encapsulated recognition of primary DMD section"""
        _raw_id = self._prime_mods_id
        dmd_secs = self.tree.findall(
            f'.//mets:dmdSec[@ID="{_raw_id}"]/mets:mdWrap/mets:xmlData/mods:mods', dfc.XMLNS)
        if len(dmd_secs) == 0:
            raise RuntimeError(f"invalid dmd_id {_raw_id}")
        # although this means invalid METS, rather check this, too
        if len(dmd_secs) > 1:
            dmd_ids = [dmd.attrib['ID'] for dmd in dmd_secs if 'ID' in dmd.attrib]
            raise RuntimeError(f"ambigious dmd_ids {dmd_ids} for {_raw_id}")
        self.primary_dmd = dmd_secs[0]

    @property
    def report(self):
        """Top level analysis"""
        if self._report is None:
            self._report = MetsReaderReport()
            self._report.identifiers = self.get_identifiers()
            self._report.system_identifiers = self._ulb_digi_system_identifier()
            self._report.languages = self.get_language_information()
            outcome = self.get_type_and_hierarchy()
            if outcome:
                if outcome[0]:
                    self._report.type = outcome[0]
                elif outcome[1]:
                    self._report.type = outcome[1]
                self._report.hierarchy = outcome[2]
            self._report.links = self.get_invalid_physical_structs()
            self._report.locations = self.get_location_shelfs()
            self._report.origins = self.get_origin_infos()
        return self._report

    def analyze(self):
        """Will be removed in future"""
        return self.report

    def insert_into_prime_mods(self, tag, attributes=None, text_content=None):
        """Insert element"""
        new_el = ET.SubElement(self.primary_dmd, tag, attrib=attributes)
        if text_content:
            new_el.text = text_content

    def check(self):
        """Ensure certain invariants

        * each logical section is at least connected to a physical container
          if not, this means empty logical sections => DDB warning, Derivans death

        Traverse only logical structure below current main logical struct 
        identified by primary descriptive section.

        raises: RuntimeError if any check fails
        """

        self._check_logical_interlinking()

    def _check_logical_interlinking(self):
        _log_ids = self.tree.xpath(f'.//mets:div[@DMDID="{self.dmd_id}"]/mets:div/@ID',
                                   namespaces=dfc.XMLNS)
        for _log_id in _log_ids:
            _links = self.tree.findall(f'.//mets:smLink[@{XLINK_FROM}="{_log_id}"]', dfc.XMLNS)
            if not _links:
                raise RuntimeError(f"{self.tree.base} no link for logical section:'{_log_id}'!")

    def get_type_and_hierarchy(self) -> Tuple:
        """
        Determine this item's type
        - inspect attribute TYPE of logical structMap
        - search pica entries
          https://www.dnb.de/SharedDocs/Downloads/EN/Professionell/Metadatendienste/linkedDataModellierungTiteldaten.pdf
        """

        # inspect logical structure
        (log_type, hierarchy) = self._determine_type_by_struct_logical()

        # inspect according dmd section
        pica_type = self._determine_by_dmd()

        return (pica_type, log_type, hierarchy)

    def _determine_by_dmd(self):
        """
        plain top-root .//*[@recordSyntax] will fail for multivolume single volumes because
        they might have a mods:relatedItem[@type="host"] that would also match
        """

        xp_vls_ext_type = 'mods:extension/*[@recordSyntax="pica"]'
        record_el = self.primary_dmd.find(xp_vls_ext_type, dfc.XMLNS)
        if record_el is not None:
            if 'code' in record_el.attrib:
                pica_code = record_el.attrib['code']
            else:
                pica_code = record_el.text
            return pica_code[0].upper() + pica_code[1:]
        return None

    def _determine_type_by_struct_logical(self):
        """determine logical type for source and hierarchy level"""

        xpath = f'.//*[@DMDID="{self._prime_mods_id}"]'
        log_el = self.findall(xpath, get_all=False)
        if log_el is not None:
            log_type = log_el.attrib['TYPE']
            _mods_id: str = self._prime_mods_id
            if _mods_id.startswith('md'):
                _mods_id = _mods_id[2:]
            _line = [(_mods_id, log_type)]
            level_up = log_el.getparent()
            while level_up.attrib['TYPE'] != 'LOGICAL':
                # strange issue with menadoc VL-instance
                # when OAI-URL misses context-path
                # where DMDID vanishes when stored local
                _id = 'n.a.'
                if 'DMDID' in level_up.attrib:
                    dmd_id = level_up.attrib['DMDID']
                    # somehow not elegant, but works
                    # to strip letters from legacy vk dmd:id
                    _id = dmd_id.strip('mdaslog')
                # take special care of newspaper hierarchy
                # which differs structurally
                # between mets and zmets
                # ATTENZIONE
                # take also care of Kitodo3 uuid identifiers
                # therefore this logic only applies to semantics (VLS)
                elif log_type in NEWSPAPER_TYPES and not level_up.attrib['ID'].startswith('uuid'):
                    _id = level_up.attrib['ID'].strip('log')
                else:
                    mptr_curr = level_up.findall('mets:mptr', dfc.XMLNS)
                    # exported METS from kitodo3
                    # using mptr to link parents
                    if len(mptr_curr) == 1:
                        _id = mptr_curr[0].attrib[XLINK_HREF]
                _type = level_up.attrib['TYPE']
                _line.append((_id, _type))
                level_up = level_up.getparent()
            return (log_type, _line)
        return (None, None)

    def _get_dmd_identifier(self):
        """ATTENTION!
        this will ONLY work with VL METS, when this workflows has processed
        the OAI-response data
        """
        agent_notes = self.tree.xpath('.//mets:agent/mets:note/text()', namespaces=dfc.XMLNS)
        if agent_notes:
            migration_note = [n for n in agent_notes if 'vlid' in n]
            if migration_note:
                return migration_note[0].split()[2]
        return None

    def get_identifiers(self):
        """
        Determine identifier for digital object from MODS and METS
        ATTENTION
        Requires Identifiers to be present, but this may change
        in future!
        """

        _identifiers = {**self._identifiers_from_prime_mods()}
        if not _identifiers:
            raise RuntimeError(f"no record _identifiers for {self.tree.base}!")
        if _identifiers and self._config:
            self._validate_identifier_types(_identifiers)
        return _identifiers

    def _identifiers_from_prime_mods(self) -> dict:
        """Collect identifiers and sources from prime MODS"""
        _identifiers = {}
        top_idents = self.primary_dmd.findall('mods:identifier', dfc.XMLNS)
        for top_ident in top_idents:
            _identifiers[top_ident.attrib['type']] = top_ident.text
        record_infos = self.primary_dmd.findall('mods:recordInfo/mods:recordIdentifier', dfc.XMLNS)
        for rec in record_infos:
            _identifiers[rec.attrib['source']] = rec.text
        return _identifiers

    def _ulb_digi_system_identifier(self) -> dict:
        """Determine system identifier(s)
        * use METS-agent-information, if available

        Please note:
        Due migration efforts there can be more
        than one system id present: actual plus 
        legacy _identifiers
        """
        ident_dict = {}
        repositories = []
        # inspect mets header
        _mhdrs = self.xpath('//mets:metsHdr')
        if len(_mhdrs) == 1:
            _mhdr = _mhdrs[0]
            repositories = self.xpath('mets:agent[@OTHERTYPE="REPOSITORY"]/mets:name/text()', _mhdr)
        repo_one = repositories[0] if len(repositories) == 1 else MARK_AGENT_LEGACY.split(':', maxsplit=1)[0]
        # legacy migrated record found?
        _legacy_marks = self.xpath(f'//mets:note[contains(text(), "{MARK_AGENT_LEGACY}")]/text()')
        if len(_legacy_marks) == 1:
            _id = _legacy_marks[0]
            if ':' in _id:
                _legacy_ident = _id.rsplit(':', maxsplit=1)[1].strip()
                _legacy_ident = _legacy_ident[2:] if _legacy_ident.startswith(
                    'md') else _legacy_ident
                ident_dict[repo_one] = _legacy_ident
            else:
                ident_dict[repo_one] = _id
        _vls_marks = self.xpath(f'//mets:note[contains(text(), "{MARK_AGENT_VLID}")]/text()')
        if len(_vls_marks) == 1:
            _id = _vls_marks[0][len(MARK_AGENT_VLID):].strip()
            ident_dict[repo_one] = _id
        # legacy vls record which is not mapped by now?
        if repo_one and ('digital' in repo_one or 'menadoc' in repo_one) and repo_one not in ident_dict:
            _legacy_id = self.dmd_id[2:] if self.dmd_id.startswith('md') else self.dmd_id
            ident_dict[repo_one] = _legacy_id
        # legacy kitodo2 source _without_ OAI envelope
        _creators = self.tree.xpath(
            '//mets:agent[@OTHERTYPE="SOFTWARE" and @ROLE="CREATOR"]/mets:name', namespaces=dfc.XMLNS)
        if len(_creators) == 1 and 'kitodo-ugh' in _creators[0].text.lower():
            ident_dict[MARK_KITODO2] = None
        # kitodo3 metsDocumentID?
        _doc_ids = self.xpath('//mets:metsDocumentID/text()', _mhdr)
        if len(_doc_ids) == 1:
            ident_dict[MARK_KITODO3] = _doc_ids[0]
        # once migrated, now hosted at opendata
        viewer_pres = self.tree.xpath(
            './/dv:presentation[contains(./text(), "://opendata")]/text()', namespaces=dfc.XMLNS)
        if len(viewer_pres) == 1 and 'simple-search' not in viewer_pres[0]:
            ident_dict[viewer_pres[0].split('/')[2]] = viewer_pres[0]
        # return what has been learned
        return {**ident_dict, **self.report.system_identifiers}

    def _validate_identifier_types(self, _identifiers: dict):
        """Transform all known _identifiers to match configuration
        regarding digital lifecycle"""

        if not self.config or not self.config.has_section('identifier'):
            return _identifiers
        _transformed = {}
        _ident_opts = self.config.options('identifier')
        for _opt in _ident_opts:
            _v = self.config.get('identifier', _opt)
            if _v in _transformed:
                raise RuntimeError(f"identifier category {_v} already in {_transformed}!")
            for k, v in _identifiers.items():
                _k = k.replace('/', '.') if '/' in k else k
                if _opt.endswith('_'+_k):
                    _transformed[_v] = v
        if len(_transformed) != len(_identifiers):
            raise RuntimeError(
                f"Identifier transformation missmatch: {_identifiers} != {_transformed}")
        return _transformed

    def get_language_information(self):
        """Read language information"""

        xp_lang_term = 'mods:language/mods:languageTerm/text()'
        return self.primary_dmd.xpath(xp_lang_term, namespaces=dfc.XMLNS)

    def get_location_shelfs(self):
        """get data for physical prints if available; might contain multiple entries"""

        xpr_signature = f'.//mets:dmdSec[@ID="{self._prime_mods_id}"]//mods:shelfLocator/text()'
        return self.tree.xpath(xpr_signature, namespaces=dfc.XMLNS)

    def get_filegrp_info(self, group='MAX') -> typing.List[METSFileInfo]:
        """Gather resource information for given filegroup
        concerning URL, MIMETYPE and container ID"""

        xpath = f'.//mets:fileGrp[@USE="{group}"]/mets:file/mets:FLocat'
        the_files = self.tree.findall(xpath, dfc.XMLNS)
        the_info = []
        for a_file in the_files:
            the_parent = a_file.getparent()
            parent_id = the_parent.attrib["ID"]
            the_type = the_parent.get("MIMETYPE", "image/jpg")
            the_loc = a_file.attrib[XLINK_HREF]
            the_info.append(METSFileInfo(file_id=parent_id,
                                         file_type=the_type,
                                         loc_url=the_loc))
        return the_info

    def get_invalid_physical_structs(self):
        """
        Check if invalid interlinkings from logical elements
        to physical structures exist
        * keep track of preceeding valid link for later fix
        """

        xpath_links = './/mets:smLink'
        xpath_phys_pattern = './/*[@ID="{}"]'
        links = self.tree.findall(xpath_links, dfc.XMLNS)

        miss_targets = []
        latest_working = None
        for link in links:
            link_from = link.attrib[XLINK_FROM]
            link_to = link.attrib[XLINK_TO]
            xpath_phys = xpath_phys_pattern.format(link_to)
            phys_el = self.tree.find(xpath_phys, dfc.XMLNS)
            if phys_el is None:
                miss_targets.append((link_from, link_to, latest_working))
            else:
                latest_working = link_to

        return miss_targets

    def get_origin_infos(self):
        """Gather informations about origin places"""

        xp_origins = 'mods:originInfo'
        xp_place_term = 'mods:place/mods:placeTerm/text()'
        origins = self.primary_dmd.findall(xp_origins, dfc.XMLNS)

        infos = []
        for origin in origins:
            place_labels = origin.xpath(xp_place_term, namespaces=dfc.XMLNS)
            for place_label in place_labels:
                if 'eventType' in origin.attrib:
                    infos.append((origin.attrib['eventType'], place_label))
                else:
                    infos.append(('n.a.', place_label))
        return infos

    @property
    def dmd_id(self):
        """Get dmd_id as-it-is"""
        return self._prime_mods_id
