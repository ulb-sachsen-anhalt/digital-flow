"""Metadata module regarding METS/MODS"""

import abc
import collections
import dataclasses
import os
import time
import typing

from pathlib import Path

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


class DigiflowMetadataException(Exception):
    """Mark errors arising from digiflow
    metadata analysis and processing"""


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

    def __init__(self, input_xml, xmlns=None):
        self.root = None
        self.path_xml = dfc.UNSET_LABEL
        if isinstance(input_xml, ET._Element):
            self.root = input_xml
        else:
            if isinstance(input_xml, str):
                input_xml = Path(input_xml)
            if not input_xml.is_file():
                raise DigiflowMetadataException(f"{input_xml} considered to be a path but is not!")
            self.path_xml = input_xml
            self.xmlns = xmlns
            if not self.xmlns:
                self.xmlns = dfc.XMLNS
            self.path_xml_dir = os.path.dirname(input_xml)
            self._parse()

    def _parse(self):
        self.root = ET.parse(self.path_xml).getroot()

    def remove(self, tags):
        """remove elements by tagname"""

        for tag in tags:
            removals = self.root.findall(f'.//{tag}', self.xmlns)
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
            els = self.root.findall(expression, dfc.XMLNS)
        if els is None:
            return []
        if not get_all:
            return els[0]
        return els

    def xpath(self, expression, element=None):
        """wrap xpath 1.0 calls"""
        if element is not None:
            return element.xpath(expression, namespaces=dfc.XMLNS)
        return self.root.xpath(expression, namespaces=dfc.XMLNS)

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
                raise DigiflowMetadataException(f'invalid write dir {out_dir}')
            mets_dir = out_dir

        path_out = os.path.join(mets_dir, file_name)
        write_xml_file(self.root, path_out)
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

        mets_hdr: ET._Element = self.root.find('.//mets:metsHdr', dfc.XMLNS)
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

    def contains_group(self, group: str) -> bool:
        """Test if a certain fileGroup exists"""

        if isinstance(group, list):
            return any(self.root.findall(PATTERN_FILEGROUP_USE.format(g), dfc.XMLNS)
                       for g in group)
        found_group = self.root.findall(PATTERN_FILEGROUP_USE.format(group), dfc.XMLNS)
        return len(found_group) > 0

    def clear_filegroups(self, black_list=None):
        """Clear file Groups by blacklist"""

        cleared = 0
        file_sections = self.root.findall('.//mets:fileSec', dfc.XMLNS)
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
            references = self.root.findall(
                f'.//mets:fptr[@FILEID="{file_id}"]', dfc.XMLNS)
            for reference in references:
                parent = reference.getparent()
                parent.remove(reference)


@dataclasses.dataclass
class DmdReport:
    """Information about descriptive
    METS metadata sections alike MODS
    """

    dmd_id: str
    is_prime: bool
    type = None
    licence = None
    identifiers = {}
    related = []
    languages = []
    locations = []
    origins = []


@dataclasses.dataclass
class MetsReport:
    """Information about digital object"""

    system_identifier = None
    type = None
    prime_report = None
    dmd_reports = []
    hierarchy = []
    links = None
    files = {}


PREFIX_VLS = 'md'

NEWSPAPER_TYPES = ['issue', 'additional', 'year', 'newspaper']


class MetsReader(MetsProcessor):
    """
    Read and try to make sense of METS/MODS Metadata which
    might be enclosed in OAI-PMH or as local file.

    Pass a File|Path|XMLTree object as target for examination.

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

    def __init__(self, input_mets, dmd_id: str = None):
        super().__init__(input_mets)
        self.primary_dmd = None
        self._prime_mods_id = dmd_id
        if self._prime_mods_id is None:
            self._find_prime_mods_id()
        self._set_prime_dmd()
        self._report: typing.Optional[MetsReport] = None
        self._config = None

    def _find_prime_mods_id(self):
        """
        Inspect METS
        * if only single MODS present, take it
        * if more present, only care for sections with identifiers
          (ignore Kitodo's additional title sections)
        * otherwise, guess prime by structure map
        """

        dmd_candidates = self.root.findall(XPR_MODS_SEC, dfc.XMLNS)
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
        log_struct = self.root.find('.//mets:structMap[@TYPE="LOGICAL"]', dfc.XMLNS)
        if log_struct is None:
            raise DigiflowMetadataException(f"No logical struct in {self.root.base}!")
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
                    _root_link = self.root.find(_xpr_root_link, dfc.XMLNS)
                    if _root_link is not None:
                        _log_id = _root_link.attrib[f'{{{dfc.XMLNS["xlink"]}}}from']
                    # no physical root link found
                    # now map most frequent xlink:from reference
                    # this must be the logical root
                    else:
                        _xpr_link = './/mets:structLink/mets:smLink'
                        _links = self.root.findall(_xpr_link, dfc.XMLNS)
                        _link_map = collections.defaultdict(int)
                        for _link in _links:
                            _from = _link.attrib[f'{{{dfc.XMLNS["xlink"]}}}from']
                            _link_map[_from] += 1
                        # now sort entries
                        # get first match which must be the "top-linker"
                        _log_id = sorted(_link_map, key=lambda e: e[1])[0]
                    # ... so
                    # we shall know by now the top log id
                    _xpr_log = f'.//mets:structMap[@TYPE="LOGICAL"]//mets:div[@ID="{_log_id}"]'
                    _log = self.root.find(_xpr_log, dfc.XMLNS)
                    if _log is not None:
                        _raw_id = _log.attrib['DMDID']
        # if still no primary id found, go nuts
        if not _raw_id:
            raise DigiflowMetadataException(f"Can't find primary dmd_id in {self.root.base}")
        return _raw_id

    def _set_prime_dmd(self):
        """Encapsulated recognition of primary DMD section"""
        the_id = self._prime_mods_id
        dmd_secs = self.root.findall(
            f'.//mets:dmdSec[@ID="{the_id}"]/mets:mdWrap/mets:xmlData/mods:mods', dfc.XMLNS)
        if len(dmd_secs) == 0:
            raise DigiflowMetadataException(f"invalid dmd_id {the_id}")
        # although this means invalid METS, rather check this, too
        if len(dmd_secs) > 1:
            dmd_ids = [dmd.attrib['ID'] for dmd in dmd_secs if 'ID' in dmd.attrib]
            raise DigiflowMetadataException(f"ambigious dmd_ids {dmd_ids} for {the_id}")
        self.primary_dmd = dmd_secs[0]

    @property
    def report(self):
        """Top level analysis"""
        if self._report is None:
            self._report = MetsReport()
            self._report.system_identifier = self.ulb_system_identifier()
            prime_reader = ModsReader(self.primary_dmd, self._prime_mods_id)
            prime_report: DmdReport = prime_reader.report
            self._report.type, self._report.hierarchy = self.inspect_logical_struct()
            self._report.links = self.get_invalid_physical_structs()
            self._report.dmd_reports.append(prime_report)
            self._report.prime_report = prime_report
        return self._report

    def analyze(self):
        """Will be removed in future"""
        return self.report

    def insert_into_prime_mods(self, tag, attributes=None, text_content=None):
        """Insert element"""
        new_el = ET.SubElement(self.primary_dmd, tag, attrib=attributes)
        if text_content:
            new_el.text = text_content

    def inspect_logical_struct_links(self):
        """Ensure each logical section is at least connected to a physical
        container. If not, this means an orphan logical sections. Although
        it only yields a DDB-validation warning, Derivans will die if
        it tries to create a PDF outline.

        Done by traversing logical structure below current main logical
        section, identified by primary descriptive section.

        raises: DigiflowMetadataException if any check fails
        """

        log_ids = self.root.xpath(f'.//mets:div[@DMDID="{self.dmd_id}"]/mets:div/@ID',
                                   namespaces=dfc.XMLNS)
        for log_id in log_ids:
            all_links = self.root.findall(f'.//mets:smLink[@{XLINK_FROM}="{log_id}"]', dfc.XMLNS)
            if not all_links:
                raise DigiflowMetadataException(
                    f"{self.root.base} no link for logical section:'{log_id}'!")

    def inspect_logical_struct(self) -> typing.Tuple:
        """Determine logical type of digital object and optional 
        hierarchy level up to farthest logical parent node (=mets:structMap)
        by inspecting the TYPE attribute of each container.

        For actual labelling cf.
        https://www.dnb.de/SharedDocs/Downloads/EN/Professionell/Metadatendienste/linkedDataModellierungTiteldaten.pdf
        """

        xpath = f'.//*[@DMDID="{self._prime_mods_id}"]'
        log_el = self.findall(xpath, get_all=False)
        if log_el is not None:
            curr_log_type = log_el.attrib['TYPE']
            the_id: str = self._prime_mods_id
            if the_id.startswith('md'):
                the_id = the_id[(len("md")):]
            nxt_prnt = log_el.getparent()
            nxt_ntr = []
            while nxt_prnt.attrib['TYPE'] != 'LOGICAL':
                # strange issue with menadoc VL-instance
                # when OAI-URL misses context-path
                # where DMDID vanishes when stored local
                log_type = nxt_prnt.attrib['TYPE']
                the_id = nxt_prnt.attrib["ID"]
                if 'DMDID' in nxt_prnt.attrib:
                    the_id = nxt_prnt.attrib['DMDID']
                    if the_id.startswith("md"): # strip from VLS dmdid
                        the_id = the_id[(len("md")):]
                    if the_id.startswith("mdaslog"): # strip from VLS legacy menadoc
                        the_id = the_id[(len("mdaslog")):]
                # take special care of newspaper hierarchy
                # which differs between vls:mets and vls:zmets
                # ATTENZIONE
                # take care of Kitodo3 uuid identifiers
                # therefore this only applies to semantics (VLS)
                elif log_type in NEWSPAPER_TYPES and not nxt_prnt.attrib['ID'].startswith('uuid'):
                    the_id = nxt_prnt.attrib['ID'].strip('log')
                else:
                    mptr_curr = nxt_prnt.findall('mets:mptr', dfc.XMLNS)
                    # exported METS from kitodo
                    # using mptr to link parents
                    if len(mptr_curr) == 1:
                        if XLINK_HREF in mptr_curr[0].attrib:
                            raw_link = mptr_curr[0].attrib[XLINK_HREF]
                            if len(raw_link.strip()) > 0:
                                the_id = raw_link.strip()
                nxt_ntr.append((the_id, log_type))
                nxt_prnt = nxt_prnt.getparent()
            return (curr_log_type, nxt_ntr)
        return (None, None)

    def _get_dmd_identifier(self):
        """ATTENTION!
        this will ONLY work with VL METS, when this workflows has processed
        the OAI-response data
        """
        agent_notes = self.root.xpath('.//mets:agent/mets:note/text()', namespaces=dfc.XMLNS)
        if agent_notes:
            migration_note = [n for n in agent_notes if 'vlid' in n]
            if migration_note:
                return migration_note[0].split()[2]
        return None

    def ulb_system_identifier(self) -> dict:
        """Determine system identifier(s)
        * use METS-agent-information, if available
        Please note:
            Due migration efforts can be several system ids since the
            digital object wandered from system to system of old
        """
        ident_dict = {}
        repositories = []
        # inspect mets header
        mets_hdrs = self.xpath('//mets:metsHdr')
        if len(mets_hdrs) == 1:
            mets_hdr = mets_hdrs[0]
            repositories = self.xpath('mets:agent[@OTHERTYPE="REPOSITORY"]/mets:name/text()', mets_hdr)
        repo_one = repositories[0] if len(
            repositories) == 1 else MARK_AGENT_LEGACY.split(':', maxsplit=1)[0]
        # legacy migrated record found?
        legacy_marks = self.xpath(f'//mets:note[contains(text(), "{MARK_AGENT_LEGACY}")]/text()')
        if len(legacy_marks) == 1:
            legacy_id = legacy_marks[0]
            if ':' in legacy_id:
                legacy_ident = legacy_id.rsplit(':', maxsplit=1)[1].strip()
                legacy_ident = legacy_ident[2:] if legacy_ident.startswith(
                    'md') else legacy_ident
                ident_dict[repo_one] = legacy_ident
            else:
                ident_dict[repo_one] = legacy_id
        legacy_vls_marks = self.xpath(f'//mets:note[contains(text(), "{MARK_AGENT_VLID}")]/text()')
        if len(legacy_vls_marks) == 1:
            legacy_id = legacy_vls_marks[0][len(MARK_AGENT_VLID):].strip()
            ident_dict[repo_one] = legacy_id
        # legacy vls record which is not mapped by now?
        if repo_one and ('digital' in repo_one or 'menadoc' in repo_one) and repo_one not in ident_dict:
            legacy_id = self.dmd_id[2:] if self.dmd_id.startswith('md') else self.dmd_id
            ident_dict[repo_one] = legacy_id
        # legacy kitodo2 source _without_ OAI envelope
        agent_creators = self.root.xpath(
            '//mets:agent[@OTHERTYPE="SOFTWARE" and @ROLE="CREATOR"]/mets:name', namespaces=dfc.XMLNS)
        if len(agent_creators) == 1 and 'kitodo-ugh' in agent_creators[0].text.lower():
            ident_dict[MARK_KITODO2] = None
        # kitodo3 metsDocumentID?
        k3_doc_ids = self.xpath('//mets:metsDocumentID/text()', mets_hdr)
        if len(k3_doc_ids) == 1:
            ident_dict[MARK_KITODO3] = k3_doc_ids[0]
        # once migrated of old, now hosted at opendata
        viewer_pres = self.root.xpath(
            './/dv:presentation[contains(./text(), "://opendata")]/text()', namespaces=dfc.XMLNS)
        if len(viewer_pres) == 1 and 'simple-search' not in viewer_pres[0]:
            ident_dict[viewer_pres[0].split('/')[2]] = viewer_pres[0]
        # return what has been learned
        return ident_dict

    def get_filegrp_info(self, group='MAX') -> typing.List[METSFileInfo]:
        """Gather resource information for given filegroup
        concerning URL, MIMETYPE and container ID"""

        xpath = f'.//mets:fileGrp[@USE="{group}"]/mets:file/mets:FLocat'
        the_files = self.root.findall(xpath, dfc.XMLNS)
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

        xpath_links = ".//mets:smLink"
        xpath_phys_pattern = ".//*[@ID='{}']"
        links = self.root.findall(xpath_links, dfc.XMLNS)

        miss_targets = []
        latest_working = None
        for link in links:
            link_from = link.attrib[XLINK_FROM]
            link_to = link.attrib[XLINK_TO]
            xpath_phys = xpath_phys_pattern.format(link_to)
            phys_el = self.root.find(xpath_phys, dfc.XMLNS)
            if phys_el is None:
                miss_targets.append((link_from, link_to, latest_working))
            else:
                latest_working = link_to

        return miss_targets

    @property
    def dmd_id(self):
        """Get dmd_id of prime MODS section"""
        return self._prime_mods_id


class ModsReader(XMLProcessor):
    """Extract knowledge from METS/MODS section"""

    def __init__(self, dmd_node, dmd_id):
        super().__init__(dmd_node)
        self.root = dmd_node
        self.dmd_id = dmd_id
        self._report = None

    @property
    def report(self):
        """Access descriptive information of this section"""

        if self._report is None:
            dmd_report: DmdReport = DmdReport(self.dmd_id, is_prime=True)
            dmd_report.type = self.get_type()
            dmd_report.identifiers = self.get_identifiers()
            dmd_report.languages = self.get_language_information()
            the_shelfs = self.get_location_shelfs()
            if the_shelfs:
                dmd_report.locations = the_shelfs[0] if len(the_shelfs) == 1 else the_shelfs
            dmd_report.origins = self.get_origins()
            the_lics = self.get_licence()
            dmd_report.licence = the_lics[0] if len(the_lics) == 1 else the_lics
            the_rels = self.get_relations()
            dmd_report.related = the_rels[0] if len(the_rels) == 1 else the_rels
            self._report = dmd_report
        return self._report

    def get_type(self):
        """
        Get PICA type if present in mods:extension.
        Please note: Plain .//*[@recordSyntax] will fail for multivolume single volumes because
            they might have a mods:relatedItem[@type="host"] that would also match.
        Inspect further custom goobi:metadata@name=PICAType if present and no match so far.
        """

        xp_vls_ext_type = 'mods:extension/*[@recordSyntax="pica"]'
        record_el = self.root.find(xp_vls_ext_type, dfc.XMLNS)
        if record_el is not None:
            if 'code' in record_el.attrib:
                pica_code = record_el.attrib['code']
            else:
                pica_code = record_el.text
            return pica_code[0].upper() + pica_code[1:]
        # alternative goobi extension (Kitodo 2)
        goobi_picas = self.root.xpath("mods:extension//*[@name='PICAType']/text()",
                                      namespaces=dfc.XMLNS)
        if len(goobi_picas) == 1:
            return goobi_picas[0]
        return None

    def get_identifiers(self):
        """Read different kind-o-identifiers from DMD MODS
        and respect custom Kitodo goobi-elements
        """
        idents = {}
        top_idents = self.root.findall("mods:identifier", dfc.XMLNS)
        for top_ident in top_idents:
            idents[top_ident.attrib['type']] = top_ident.text
        record_infos = self.root.findall("mods:recordInfo/mods:recordIdentifier", dfc.XMLNS)
        for rec in record_infos:
            idents[rec.attrib['source']] = rec.text
        goobi_srcs = self.root.xpath("mods:extension//*[@name='CatalogIDSource' and not(@anchorId)]/text()",
                                     namespaces=dfc.XMLNS)
        if len(goobi_srcs) > 0:
            idents["goobi:CatalogSourceID"] = goobi_srcs[0]
        goobi_parents = self.root.xpath("mods:extension//*[@name='CatalogIDSource' and @anchorId]/text()",
                                     namespaces=dfc.XMLNS)
        if len(goobi_parents) == 1:
            idents["goobi:anchorID"] = goobi_parents[0]
        goobi_vds = self.root.xpath("mods:extension//*[starts-with(@name, 'VD')]",
                                    namespaces=dfc.XMLNS)
        if len(goobi_vds) > 0:
            for vd in goobi_vds:
                vd_key = vd.get("name")
                vd_val = str(vd.text).strip()
                idents[vd_key] = vd_val
        goobi_urns = self.root.xpath("mods:extension//*[contains(@name, 'urn')]/text()",
                                     namespaces=dfc.XMLNS)
        if len(goobi_urns) == 1:
            key = "urn"
            if ":nbn:" in goobi_urns[0]:
                key = "urn:nbn"
            idents[key] = goobi_urns[0].strip()
        if len(idents) < 1:
            raise DigiflowMetadataException(f"no identifiers in {self.dmd_id} of {self.root.base}!")
        return idents

    def get_relations(self):
        """"Inspect propably existing mods:relations"""

        related = []
        rels = self.root.findall("mods:relatedItem", dfc.XMLNS)
        for item in rels:
            the_type = item.get("type", default=dfc.UNSET_LABEL)
            rec_idents = item.findall("mods:recordInfo/mods:recordIdentifier", dfc.XMLNS)
            for rec in rec_idents:
                the_src = rec.get("source", default=dfc.UNSET_LABEL)
                the_val = rec.text
                related.append((the_type, the_src, the_val))
        return related

    def get_language_information(self):
        """Read language information"""

        xp_lang_term = "mods:language/mods:languageTerm/text()"
        return self.root.xpath(xp_lang_term, namespaces=dfc.XMLNS)

    def get_location_shelfs(self):
        """get data for physical prints if available
        might contain multiple entries
        might also differ in-depth, vls planted it of old
        in trees like holdingSimple/copyInformation/shelfLocator
        """

        langs = self.root.xpath("mods:location//mods:shelfLocator/text()",
                                namespaces=dfc.XMLNS)
        if len(langs) > 0:
            return langs
        goobi_doclangs = self.root.xpath("mods:extension//*[@name='DocLanguage']",
                                         namespaces=dfc.XMLNS)
        if len(goobi_doclangs) > 0:
            return goobi_doclangs

    def get_origins(self):
        """Gather informations about origin places"""

        xp_origins = "mods:originInfo"
        xp_place_term = "mods:place/mods:placeTerm/text()"
        xp_year = "*[local-name()='dateIssued' or local-name()='dateCaptured']"
        origins = self.root.findall(xp_origins, dfc.XMLNS)

        infos = []
        for origin in origins:
            an_event = origin.get('eventType', default="publication")
            a_year = dfc.UNSET_LABEL
            all_years = origin.xpath(xp_year, namespaces=dfc.XMLNS)
            for y in all_years:
                if "keyDate" in y.attrib:
                    a_year = y.text
                    break
            if a_year == dfc.UNSET_LABEL and len(all_years) > 0:
                a_year = all_years[0].text
            a_place = dfc.UNSET_LABEL
            place_labels = set(origin.xpath(xp_place_term, namespaces=dfc.XMLNS))
            if len(place_labels) == 1:
                a_place = place_labels.pop()
            infos.append((an_event, a_year, a_place))
        return infos

    def get_licence(self):
        """licence information if external link exists"""

        xpr_signature = "mods:accessCondition"
        accesses = self.root.findall(xpr_signature, dfc.XMLNS)
        licences = []
        if len(accesses) > 0:
            for a in accesses:
                the_type = a.get("type", default=dfc.UNSET_LABEL)
                the_link = dfc.UNSET_LABEL
                if XLINK_HREF in a.attrib:
                    the_link = a.get(XLINK_HREF)
                elif "href" in a.attrib:
                    the_link = a.get("href")
                the_txt = a.text
                licences.append((the_type, the_link, the_txt))
        return licences
