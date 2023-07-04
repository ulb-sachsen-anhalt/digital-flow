# -*- coding: utf-8 -*-
"""Module for Handling of persistent Identifiers"""

import enum
import functools
import os
import re

from collections import (
    namedtuple
)
from pathlib import (
    Path
)
from typing import (
    List,
)

from .digiflow_metadata import (
    MetsReader,
    XMLNS,
)


##################
# script constants
#
# URN PATTERN
URN_PATTERN_ULB = 'urn:nbn:de:gbv:3:{}-{}-{}-{}'
DEFAULT_PATTERN_ULB = r"^urn\:nbn\:de\:gbv\:3\:\d\-\d{3,}.*\d+$"
DEFAULT_GRANULAR_PAGE_FRAGMENT = '/fragment/page'
# kitodo2 system id
KITODO2_ID = '1192015415'

###############
# Pruefziffer mapping
# Map Chars to Ints
# cf. http://www.persistent-identifier.de/?link=316
PZ_MAP = {}
PZ_MAP["0"] = 1
PZ_MAP["1"] = 2
PZ_MAP["2"] = 3
PZ_MAP["3"] = 4
PZ_MAP["4"] = 5
PZ_MAP["5"] = 6
PZ_MAP["6"] = 7
PZ_MAP["7"] = 8
PZ_MAP["8"] = 9
PZ_MAP["u"] = 11
PZ_MAP["r"] = 12
PZ_MAP["n"] = 13
PZ_MAP["b"] = 14
PZ_MAP["d"] = 15
PZ_MAP["e"] = 16
PZ_MAP[":"] = 17
PZ_MAP["a"] = 18
PZ_MAP["c"] = 19
PZ_MAP["f"] = 21
PZ_MAP["g"] = 22
PZ_MAP["h"] = 23
PZ_MAP["i"] = 24
PZ_MAP["j"] = 25
PZ_MAP["l"] = 26
PZ_MAP["L"] = 26
PZ_MAP["m"] = 27
PZ_MAP["o"] = 28
PZ_MAP["p"] = 29
PZ_MAP["q"] = 31
PZ_MAP["s"] = 32
PZ_MAP["t"] = 33
PZ_MAP["v"] = 34
PZ_MAP["w"] = 35
PZ_MAP["x"] = 36
PZ_MAP["X"] = 36
PZ_MAP["y"] = 37
PZ_MAP["Y"] = 37
PZ_MAP["z"] = 38
PZ_MAP["Z"] = 38
PZ_MAP["-"] = 39
PZ_MAP["9"] = 41
PZ_MAP["k"] = 42
PZ_MAP["_"] = 43
PZ_MAP["/"] = 45
PZ_MAP["."] = 47
PZ_MAP["+"] = 45


class DigiFlowURNExistsException(Exception):
    """Mark URN already exists"""


class GranularURNExistsException(Exception):
    """Mark Granular URN exists"""


class GranularURNException(Exception):
    """Mark generic granular URN Exception"""


class URNType(enum.IntEnum):
    '''
    Represent different level of identifiers
    Represent URN on work-level, i.e. for a
    * work-level: identifies digitalization objects like
      monographiesy, c- and f-stages or newspaper years/issues
    * page-level: physical page with complete URN
    * page-fragment-level: physical page
        with fragemented URN derived from work-level URN
    * page-fragment-focus: box within physical page
        with fragemented URN derived from work-level URN

    Please note, that there was also a granular URN
        with corresponded to only printed pages, but this
    version was not mentioned recently
    '''

    WORK = 1
    GRANULAR_PAGE = 2
    GRANULAR_FRAGMENT_PAGE = 3
    GRANULAR_FRAGMENT_PAGE_FOCUS = 4
    MISSING = 5
    UNKNOWN = 6


def generate_urn_pz(urn):
    """
    Generate Checkdigit for persistent identifiers
    see: http://www.persistent-identifier.de/?link=316
    """

    # map + reduce
    mapped_digits_str = functools.reduce(lambda c, p: c + p, [str(PZ_MAP[c]) for c in urn])

    # sum
    pzs = sum((i * int(c) for i, c in enumerate(mapped_digits_str, 1)))

    # divide by last element of digits mapped from urn
    divisor = mapped_digits_str[-1]
    quotient = int(pzs / int(divisor))

    # return last digit
    return str(quotient)[-1:][0]


# Container Struct
GranularContainer = namedtuple('GranularContainer', ['id', 'type', 'urn'])

class ProcessReport:
    """Handle outcomes concerning URN handling"""

    def __init__(self, added:List[GranularContainer], existed:List[GranularContainer]) -> None:
        self.added: List[GranularContainer] = added
        self.existed: List[GranularContainer] = existed
        self._replaced = None

    @property
    def replaced(self):
        """Get entries for which a mapping exist
        for existed and added entries, which
        means they were replaced"""
        if self._replaced is None:
            self._replaced = {}
            for _add in self.added:
                for _exs in self.existed:
                    if _add.id == _exs.id:
                        self._replaced[_add.id] = f"{_exs.urn} => {_add.urn}"
        return self._replaced


def _determine_types(elem_cnts:List) -> List[GranularContainer]:
    _types = []
    for _el in elem_cnts:
        cnt_id = _el.attrib['ID']
        gurn = _el.get('CONTENTIDS')
        if not gurn:
            _type = URNType.MISSING
        else:
            if re.match(r".*\-p\d{4}\-\d$", gurn):
                _type = URNType.GRANULAR_PAGE
            elif re.match(r".*\/fragment\/page=\d{4,}$", gurn):
                _type = URNType.GRANULAR_FRAGMENT_PAGE
            else:
                _type = URNType.UNKNOWN
        _types.append(GranularContainer(cnt_id, _type, gurn))
    return _types


def _determine_main_type(cnts:List[GranularContainer]) -> URNType:
    _map = {}
    for cnt in cnts:
        if cnt.type in _map:
            _map[cnt.type] = _map[cnt.type] + 1
        else:
            _map[cnt.type] = 1
    return max(_map.items(), key=lambda t: t[1])[0]


def enrich_urn_granular(xml_tree,
                        urn_main,
                        page_num=None,
                        padd_left=4, do_sanitize=False, do_replace=False):
    """
    Enrich URN granular at physical containers
    default: Granular URN 2.0 - GRANULAR_FRAGMENT_PAGE
    infix '/fragment/page={fptr:href[last]}' in div@CONTENTIDS
    """

    add = []
    exs = []
    cnts_total = _get_phys_containers(xml_tree)
    cnt_types = _determine_types(cnts_total)
    exs = cnt_types
    cnt_urn_fits = [e for e in cnts_total if does_fit(e, pattern=DEFAULT_PATTERN_ULB)]
    urn_type = _determine_main_type(cnt_types)
    if do_replace:
        cnt_urn_fits = []
    cnt_diffs = len(cnts_total) - len(cnt_urn_fits)
    # if no granular urns exist at all, then all must get new urn ...
    if not cnt_urn_fits:
        # identify insertion point
        phys_divs = xml_tree.findall('.//mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div', XMLNS)
        # alert invalid data
        if len(phys_divs) == 0:
            raise GranularURNException(f"No phys pages in {xml_tree.base}!")
        # insert URNTYPE.GRANULAR_FRAGMENT_PAGE for each physical container
        for phys_div in phys_divs:
            add.append(_insert_page_fragment(phys_div, urn_main, padd_left))
        urn_type = URNType.GRANULAR_FRAGMENT_PAGE
    # if differences between phys containers exist, then only some must be
    # inserted / generated depending on existing urn type
    # might happend with additional images (colorchecker, post scans, ...)
    elif cnt_diffs:
        # here we pass physical containers, since we *might* require it
        # for sanitizing CONTENTIDS for latest added structure
        try:
            add += insert_granular_urn(urn_type, cnts_total, urn_main, page_num, padd_left)
        except GranularURNExistsException as exc:
            if do_sanitize:
                (_, _, report) = _guess_urn_granular_from_existing(xml_tree, urn_main)
                add += report.added
            else:
                raise GranularURNExistsException(exc) from exc
    _report = ProcessReport(added=add,existed=exs)
    return (xml_tree, urn_type, _report)


def _guess_urn_granular_from_existing(xml_tree, urn_main):
    """Here we run in a seldom particular failure
    driven by a missing granularen URN - let's try to fix 'em just one time!
    """
    cnts_total = _get_phys_containers(xml_tree)
    phys_divs = [e for e in cnts_total if does_fit(e, pattern=DEFAULT_PATTERN_ULB)]
    last_phys_div = cnts_total[-1]
    len_phys_div = len(cnts_total)
    if str(len_phys_div) == last_phys_div.attrib['ORDER']:
        contentids = sorted([i.attrib['CONTENTIDS'] for i in phys_divs]).pop()
        page_num = int(contentids.split('-')[-2][1:]) + 1
        xml_tree, urn_type, rep = enrich_urn_granular(xml_tree, urn_main, page_num=page_num)
        _part_rep = ProcessReport(added=rep.added, existed=[])
        return (xml_tree, urn_type, _part_rep)
    raise GranularURNException(f"Inconsistent Granular URN '{xml_tree.base}'")


def _insert_page_fragment(phys_div, main_urn: str, padd:int):
    order = int(phys_div.attrib['ORDER'])
    pf_o = f"{order:0{padd}}"
    the_gran_urn = f"{main_urn}{DEFAULT_GRANULAR_PAGE_FRAGMENT}={ pf_o}"
    phys_div.attrib['CONTENTIDS'] = the_gran_urn
    return GranularContainer(id=phys_div.attrib['ID'],
                             type=URNType.GRANULAR_FRAGMENT_PAGE,
                             urn=the_gran_urn)


def does_fit(e, pattern=DEFAULT_PATTERN_ULB):
    """
    Proper NBN URN consists of
    * prefix like urn:nbn:de:gbv:3
    * some subseqent numerical collection number
    * additional identifiers (sementic IDs, PPNs, ...)
    * suffix alike 'fragment' or '-p' for granularity
    * probably numerial exemplar count
    * final singular checkdigit
    """
    if 'CONTENTIDS' not in e.attrib:
        return False
    _id = e.attrib['CONTENTIDS']
    return re.match(pattern, _id) is not None


def insert_granular_urn(urn_type, phys_conts, main_urn, page_num=None, padd_left=4):
    """
    Take care of inconsistent physical containers!

    Physical containers missing CONTENTIDS
    http://digital.bibliothek.uni-halle.de/hd/oai/?verb=GetRecord&metadataPrefix=mets&mode=xml&identifier=10595

    Granular URN missing in between, therefore ORDER and granular URN do not match in the end
    https://digitale.bibliothek.uni-halle.de/vd16/oai?verb=GetRecord&metadataPrefix=mets&identifier=993531
    """

    inserts = []
    els_missing = [e for e in phys_conts if not does_fit(e, pattern=DEFAULT_PATTERN_ULB)]
    if urn_type == URNType.GRANULAR_PAGE:
        for el_misses in els_missing:
            # crashes if @ORDER cant be parsed as integer
            # calculate pXXXX from attr 'ORDER'
            # or, -if provided-, from parameter page_num
            p_number = page_num if page_num else int(el_misses.attrib['ORDER'])
            raw_gurn = f"{main_urn}-p{p_number:0{padd_left}}-"
            new_gurn = f"{raw_gurn}{generate_urn_pz(raw_gurn)}"
            el_id = el_misses.attrib['ID']
            # check if new granular URN is *really* unique
            # raise Exception otherwise ...
            cnts_with_gurn = [
                e for e in phys_conts
                if 'CONTENTIDS' in e.attrib and re.match(r".*-p\d+-\d$", e.attrib['CONTENTIDS'])]
            existing_ones = [e for e in cnts_with_gurn
                             if e.attrib['CONTENTIDS'] == new_gurn]
            if existing_ones:
                pat = "calculated granular URN '{}' already used by: '{}'!"
                exsting_ids = [e.attrib['ID'] for e in existing_ones]
                msg_fail = pat.format(new_gurn, exsting_ids)
                raise GranularURNExistsException(msg_fail)
            el_misses.attrib['CONTENTIDS'] = new_gurn
            inserts.append(GranularContainer(id=el_id, type=URNType.GRANULAR_FRAGMENT_PAGE, urn=new_gurn))
    return inserts


def _get_phys_containers(xml_tree):
    """Get all physical containers that contain @ORDER attribute"""
    return xml_tree.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@ORDER]', XMLNS)


def enrich_urn_kitodo2(process_path:'str|Path', collection='1', system_id=KITODO2_ID, exemplar='1',
               force_overwrite=False):
    """Enrich URN for Kitodo2 if not existing"""

    _urns = []
    read = MetsReader(process_path)
    ulb_urns = read.findall('.//goobi:metadata[@name="ulb_urn"]', read.primary_dmd)
    if ulb_urns and not force_overwrite:
        raise GranularURNExistsException(f"Detected existing URN {ulb_urns}!")

    identifiers = read.xpath('.//goobi:metadata[@name="CatalogIDSource" and(not(@anchorId))]/text()')
    if len(identifiers) != 1:
        raise GranularURNException(f"Ambigious/invalid base URN {identifiers}!")

    # format persistent identifier (aka URN) with provided info
    urn = URN_PATTERN_ULB.format(collection, system_id, identifiers[0], exemplar)

    # generate pruefziffer
    urn += generate_urn_pz(urn)

    # insert
    read.insert_into_prime_mods('{http://meta.goobi.org/v1.5.1/}metadata',
                    {'name': 'ulb_urn'}, urn)
    read.write()
    _urns.append(urn)

    # multi volume?
    process_path = os.path.dirname(process_path)
    path_meta_anchor = os.path.join(process_path, 'meta_anchor.xml')
    if os.path.exists(path_meta_anchor):
        ppn_c_stage = read.xpath('.//*[@anchorId="true"]/text()')
        urn_host = f'urn:nbn:de:gbv:3:{collection}-{system_id}-{ppn_c_stage[0]}-{exemplar}'
        pz_c_stage = generate_urn_pz(urn_host)
        urn_host += pz_c_stage
        _proc_host = MetsReader(path_meta_anchor)
        _proc_host.insert_into_prime_mods('{http://meta.goobi.org/v1.5.1/}metadata',
                    {'name': 'ulb_urn'}, urn_host)
        _proc_host.write()
        _urns.append(urn_host)
    return _urns
