# -*- coding: utf-8 -*-

import os
import shutil

import lxml.etree as ET

import pytest

from digiflow import (
    generate_urn_pz,
    enrich_urn_kitodo2,
    enrich_urn_granular,
    write_xml_file,
    does_fit,
    URNType,
    GranularURNExistsException,
    MetsReader,
    XMLNS
)

from .conftest import (
    TEST_RES
)

LABEL_META = 'meta.xml'
XPATH_GOOBI_URN = './/goobi:metadata[@name="ulb_urn"]'


@pytest.mark.parametrize(
    "test_input,expected", [
        ('urn:nbn:de:gbv:089-332175294', '5'),
        ('urn:nbn:de:gbv:3:1-1192015415-181497433-1', '9'),
        ('urn:nbn:de:gbv:3:4-1192015415-211620807-1', '7'),
        ('urn:nbn:de:gbv:3:1-62923', '0'),
        ('urn:nbn:de:gbv:3:1-69482', '1'),
        ('urn:nbn:de:gbv:3:3-21437-p0004-', '6'),
        ('urn:nbn:de:gbv:3:1-847518-1901041801', '8')])
def test_urn_checkdigits(test_input, expected):
    """
    Checkdigit algorithm (Pruefziffer)

    http://www.persistent-identifier.de/?link=316
    http://nbn-resolving.de/nbnpruefziffer.php

    ULB Schema: urn:nbn:de:gbv:3:<Sammlung>-<SystemId>-<PPN>-<ExemplarNr>
        (see: https://redmine.itz.uni-halle.de/projects/ulb-dd/wiki/URN_Generierung_Kitodo)
    """

    assert expected == generate_urn_pz(test_input)


@pytest.mark.parametrize(
    "test_input,expected", [
        ('urn:nbn:de:gbv:089-332175294-5', False),
        ('urn:nbn:de:gbv:3:1-1192015415-181497433-19', True),
        ('urn:nbn:de:gbv:n.a.', False),
        ('urn:nbn:de:gbv:3:3-21437-p0004-6', True),
        ('urn:nbn:de:gbv:3:1-694468/fragment/page=9293732', True),
        ('urn:nbn:de:gbv:3:1-847518-19010418018', True),
        ('urn:nbn:de:gbv:3:1-847518-19010418018/fragment/page=0001', True)])
def test_existing_urn_values(test_input, expected, tmp_path):
    """
    Which known URN patters are recognized as
    matching URNs?

    Please note: Granular page-related URN 2.0 are
    considered as valid URNS in this case because they are
    composed of a valid URN (with check digit)
    and following fragment extension
    """

    # arrange
    src_path = TEST_RES / "k2_mets_vd18_147638674.xml"
    path_meta = tmp_path / "147638674.xml"
    shutil.copyfile(src_path, str(path_meta))
    xml_root = ET.parse(path_meta).getroot()
    parent_phys = xml_root.find('.//mets:structMap[@TYPE="PHYSICAL"]', XMLNS)
    dummy_element = ET.SubElement(parent_phys, 'div', {'CONTENTIDS': test_input})

    # assert
    assert expected == does_fit(dummy_element)


@pytest.fixture
def fixture_k2_export(tmp_path):
    src_path = TEST_RES / "k2_mets_vd18_147638674.xml"
    path_meta = tmp_path / "147638674.xml"
    shutil.copyfile(src_path, str(path_meta))
    return str(path_meta)


def test_urn_granular_exported_from_kitodo2_default(fixture_k2_export):
    """Ensure padd_left is respected"""

    # arrange
    _urn_main = 'urn:nbn:de:gbv:3:1-1192015415-147638674-17'
    xml_root = ET.parse(fixture_k2_export).getroot()

    # act
    (xml_root_result, kind, report) = enrich_urn_granular(xml_root,
                                                       urn_main=_urn_main,
                                                       padd_left=1)
    write_xml_file(xml_root_result, fixture_k2_export)

    # assert
    assert len(report.added) == 17
    assert kind == URNType.GRANULAR_FRAGMENT_PAGE
    result_xml = ET.parse(fixture_k2_export).getroot()
    phys_divs = result_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div', XMLNS)
    for phys_div in phys_divs:
        assert phys_div.attrib['CONTENTIDS']

    urn = f'{_urn_main}/fragment/page=1'
    assert urn == phys_divs[0].attrib['CONTENTIDS']
    urn2 = f'{_urn_main}/fragment/page=17'
    assert urn2 == phys_divs[16].attrib['CONTENTIDS']


def test_urn_granular_exported_with_padding(fixture_k2_export):
    """Behavior if page padding is fixed to '4'"""

    # arrange
    _urn_main = 'urn:nbn:de:gbv:3:1-1192015415-147638674-17'
    if not os.path.exists(fixture_k2_export):
        raise ValueError(f'invalid path "{fixture_k2_export}"')
    xml_root = ET.parse(fixture_k2_export).getroot()

    # act
    (xml_root_result, kind, rep) = enrich_urn_granular(xml_root, _urn_main, padd_left=4)
    write_xml_file(xml_root_result, fixture_k2_export)

    # assert
    assert len(rep.added) == 17
    assert kind == URNType.GRANULAR_FRAGMENT_PAGE
    result_xml = ET.parse(fixture_k2_export).getroot()
    phys_divs = result_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div', XMLNS)
    urn = f'{_urn_main}/fragment/page=0001'
    assert urn == phys_divs[0].attrib['CONTENTIDS']
    urn2 = f'{_urn_main}/fragment/page=0017'
    assert urn2 == phys_divs[16].attrib['CONTENTIDS']


@pytest.fixture
def fixture_k2_plain_monography(tmp_path):
    src_path = TEST_RES / "kitodo2-goobi" / "monography" / "meta_empty_structs.xml"
    path_meta = tmp_path / LABEL_META
    shutil.copyfile(src_path, str(path_meta))
    return str(path_meta)


def test_kitodo2_monography_enrich_urn(fixture_k2_plain_monography):
    """Ensure expected URN generated and persisted"""

    # act
    result = enrich_urn_kitodo2(fixture_k2_plain_monography)

    # assert
    assert len(result) == 1
    result_xml = ET.parse(fixture_k2_plain_monography).getroot()
    urn = result_xml.find(XPATH_GOOBI_URN, XMLNS)
    assert "urn:nbn:de:gbv:3:1-1192015415-269210156-14" == urn.text


@pytest.fixture
def fixture_k2_monography(tmp_path):
    src_path = TEST_RES / "kitodo2-goobi" / "monography" / "meta.xml"
    path_meta = tmp_path / LABEL_META
    shutil.copyfile(src_path, str(path_meta))
    return str(path_meta)


def test_create_urn_granular_within_kitodo2(fixture_k2_monography):

    # arrange
    _urn_main = 'urn:nbn:de:gbv:3:3-1192015415-882674978-12'
    if not os.path.exists(fixture_k2_monography):
        raise ValueError(f'invalid path "{fixture_k2_monography}"')
    xml_root = ET.parse(fixture_k2_monography).getroot()

    # act
    (xml_root_result, kind, rep) = enrich_urn_granular(xml_root, urn_main =_urn_main, padd_left=8)
    write_xml_file(xml_root_result, fixture_k2_monography)

    # assert
    assert len(rep.added) == 27
    assert kind == URNType.GRANULAR_FRAGMENT_PAGE
    result_xml = ET.parse(fixture_k2_monography).getroot()
    phys_divs = result_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div', XMLNS)
    for phys_div in phys_divs:
        assert phys_div.attrib['CONTENTIDS']

    urn = f'{_urn_main}/fragment/page=00000001'
    assert urn == phys_divs[0].attrib['CONTENTIDS']
    urn1 = f'{_urn_main}/fragment/page=00000017'
    assert urn1 == phys_divs[16].attrib['CONTENTIDS']


def test_urn_mvw_kitodo2(tmp_path):
    """Insert expected URNs both for volume and host"""

    # arrange
    src_meta = TEST_RES / "kitodo2-goobi" / "mvw" / "meta.xml"
    path_meta = tmp_path / LABEL_META
    shutil.copyfile(src_meta, str(path_meta))
    src_anchor = TEST_RES / "kitodo2-goobi" / "mvw" / "meta_anchor.xml"
    path_anchor = tmp_path / "meta_anchor.xml"
    shutil.copyfile(src_anchor, str(path_anchor))
    dir_name = os.path.dirname(path_meta)

    # act
    enrich_urn_kitodo2(path_meta)

    # assert
    meta_xml = ET.parse(path_meta).getroot()
    urn = meta_xml.find(XPATH_GOOBI_URN, XMLNS)
    assert "urn:nbn:de:gbv:3:1-1192015415-325168768-18" == urn.text
    anchor_xml = ET.parse(os.path.join(
        dir_name, 'meta_anchor.xml')).getroot()
    urn_host = anchor_xml.find(XPATH_GOOBI_URN, XMLNS)
    assert "urn:nbn:de:gbv:3:1-1192015415-147173272-15" == urn_host.text


@pytest.fixture
def fixture_vls_monography_granular1(tmp_path):
    src_meta = TEST_RES / "vls" / "monography" / "737429-migrated.xml"
    path_meta = tmp_path / "737429.xml"
    shutil.copyfile(src_meta, str(path_meta))
    return str(path_meta)


def test_urn_granular_upsert_legacy_vls(fixture_vls_monography_granular1):
    """
    Ensure: Digitalizates with existings granular URNs preserve
    their URN-Type and that granular URNs are added if missing on
    physical containers
    """

    # arrange
    urn_main = 'urn:nbn:de:gbv:3:3-21437'
    xml_root = ET.parse(fixture_vls_monography_granular1).getroot()

    # act
    (xml_root_result, _type, rep) = enrich_urn_granular(xml_root, urn_main=urn_main)
    write_xml_file(xml_root_result, fixture_vls_monography_granular1)

    # assert
    assert os.path.exists(fixture_vls_monography_granular1)
    assert len(rep.added) == 1
    assert _type == URNType.GRANULAR_PAGE
    meta_xml = ET.parse(fixture_vls_monography_granular1).getroot()
    page5 = meta_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@CONTENTIDS]', XMLNS)[4]
    assert "urn:nbn:de:gbv:3:3-21437-p0005-2" == page5.attrib['CONTENTIDS']


def test_urn_granular_with_inconsistent_contentids(tmp_path):
    """
    ensure that inconsistent legacy data can be partially be
    helped by adding a new granular urn which would collide
    with an existing urn

    http://digital.bibliothek.uni-halle.de/hd/oai/?verb=GetRecord&metadataPrefix=mets&mode=xml&identifier=10595
    """

    # arrange
    src_meta = TEST_RES / "vls" / "monography" / "10595.xml"
    path_meta = tmp_path / "10595.xml"
    shutil.copyfile(src_meta, str(path_meta))
    xml_root = ET.parse(str(path_meta)).getroot()

    # act
    with pytest.raises(GranularURNExistsException) as execinfo:
        enrich_urn_granular(xml_root, urn_main='urn:nbn:de:gbv:3:3-178')
        assert 'calculated granular URN ' in execinfo.value


def test_urn_granular_vd18_mvw_fstage(tmp_path):
    """
    ensure that existing granular 1.0 is preserved
    """

    # arrange
    main_urn = 'urn:nbn:de:gbv:3:1-635986'
    src_meta = TEST_RES / "migration" / "9427337.mets.xml"
    path_meta = tmp_path / "9427337.mets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    xml_root = ET.parse(str(path_meta)).getroot()

    # act
    (xml_root_result, _type, rep) = enrich_urn_granular(xml_root, main_urn)
    write_xml_file(xml_root_result, str(path_meta))

    # assert
    assert os.path.exists(str(path_meta))
    assert len(rep.added) == 0
    assert _type == URNType.GRANULAR_PAGE
    meta_xml = ET.parse(str(path_meta)).getroot()
    page5 = meta_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@CONTENTIDS]', XMLNS)[1]
    assert "urn:nbn:de:gbv:3:1-635986-p0002-0" == page5.attrib['CONTENTIDS']


@pytest.fixture
def fixture_granular_urn_order_gap(tmp_path):
    """
    create complex invalid example with legacy data, where a granular URN
    seems to be missing in between pages, like it jumps from p0001 to p0003:
    """

    src_meta = TEST_RES / "vls" /"12504.mets.xml"  # mets with order gaps
    path_meta = tmp_path / "12504.mets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    xml_root = ET.parse(str(path_meta)).getroot()
    last_phys_cnt = xml_root.find('.//mets:*[@ID="phys58793"]', XMLNS)
    parent = last_phys_cnt.getparent()
    # 2. insert brand new colorchecker as last element
    attribs = {
        'ID': 'phys58794',
        'TYPE': 'page',
        'CONTENTIDS': 'urn:n.a.',
        'ORDER': '13',
        'ORDERLABEL': '[Colorchecker]'}
    ET.SubElement(parent, '{http://www.loc.gov/METS/}div', attribs)
    write_xml_file(xml_root, path_meta)
    return str(path_meta), xml_root


def test_urn_granular_order_gaps_repair(
        tmp_path, fixture_granular_urn_order_gap):
    """
    Behavior for inconsistent legacy data with gaps
    in their ORDER

    11 pages do already have a granular URN, the last one misses it
    Since the ORDER-attribute leaps from "1" to "3" for second page,
    it can't use the regular way to count the last ORDER-attribute from
    last page up by one, since the resulting URN is already owned
    by the pre-last page
    """
    # arrange
    main_urn = 'urn:nbn:de:gbv:3:1-1488'
    xml_root = fixture_granular_urn_order_gap[1]
    valid_urns_at_start = 12

    # pre-check
    cnt_ids = xml_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]//mets:div[@ORDER]', XMLNS)
    assert len([e for e in cnt_ids if '-p00' in e.get('CONTENTIDS')]) == valid_urns_at_start

    # act I
    with pytest.raises(GranularURNExistsException) as exist_exc:
        enrich_urn_granular(xml_root, main_urn)

    # assert
    assert "granular URN 'urn:nbn:de:gbv:3:1-1488-p0013-5' already used" in str(exist_exc.value)

    # act II: try to repair this time
    # (xml_res, _, ins) = enrich_urn_granular_from_contentids(fixture_granular_urn_order_gap[1])
    (xml_res, _, rep) = enrich_urn_granular(fixture_granular_urn_order_gap[1], urn_main=main_urn, do_sanitize=True)
    path_meta = tmp_path / "12504_repair.mets.xml"
    write_xml_file(xml_res, str(path_meta))
    colorchecker = xml_res.find('.//mets:*[@ID="phys58794"]', XMLNS)
    attr = colorchecker.attrib

    # assert
    assert len(rep.added) == 1
    assert attr['CONTENTIDS'] == 'urn:nbn:de:gbv:3:1-1488-p0014-1'
    assert attr['ORDER'] == '13'
    assert attr['ORDERLABEL'] == '[Colorchecker]'

    # act
    cnt2 = xml_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]//mets:div[@ORDER]', XMLNS)

    # assert
    assert len([e for e in cnt2 if '-1488-p00' in e.attrib['CONTENTIDS']]) == (valid_urns_at_start + 1)


@pytest.fixture
def fixture_granular_urn_false_order(tmp_path):
    """
    create complex invalid example with legacy data, where a granular URN
    seems to be missing in between pages, like it jumps from p0001 to p0003:
    """
    # mets with order mismatch
    src_meta = TEST_RES / "vls" / "4066583.mets.xml"
    path_meta = tmp_path / "4066583.mets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    xml_root = ET.parse(str(path_meta)).getroot()
    last_phys_cnt = xml_root.find('.//mets:*[@ID="phys4116796"]', XMLNS)
    parent = last_phys_cnt.getparent()
    # 2. insert brand new colorchecker as last element
    attribs = {
        'ID': 'phys4116797',
        'TYPE': 'page',
        'CONTENTIDS': 'urn:n.a.',
        'ORDER': '71',
        'ORDERLABEL': '[Colorchecker]'}
    ET.SubElement(parent, '{http://www.loc.gov/METS/}div', attribs)

    return str(path_meta), xml_root


def test_urn_granular_false_order(
        tmp_path, fixture_granular_urn_false_order):
    """
    check behavior for inconsistent legacy with gaps in in granular urns
    and partly missorted urn
    when new container is attached to end, like it is with colorchecker
    data
    """
    # arrange
    xml_root = fixture_granular_urn_false_order[1]
    n_valid_urns = 70

    # pre-check
    cnt_ids = xml_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]//mets:div[@ORDER]', XMLNS)
    urns = [e for e in cnt_ids if '-272189-p00' in e.attrib['CONTENTIDS']]
    assert len(urns) == n_valid_urns

    # act II: try to repair this time
    xml_res, _, rep = enrich_urn_granular(
        fixture_granular_urn_false_order[1],
        'urn:nbn:de:gbv:3:1-272189', do_sanitize=True)
    path_meta = tmp_path / "4066583_repair.mets.xml"
    write_xml_file(xml_res, str(path_meta))
    colorchecker = xml_res.find('.//mets:*[@ID="phys4116797"]', XMLNS)
    attr = colorchecker.attrib

    # assert
    assert len(rep.added) == 1
    assert attr['CONTENTIDS'] == 'urn:nbn:de:gbv:3:1-272189-p0072-1'
    assert attr['ORDER'] == '71'
    assert attr['ORDERLABEL'] == '[Colorchecker]'

    # act
    cnt2 = xml_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]//mets:div[@ORDER]', XMLNS)

    # assert after enrich
    urns = [e for e in cnt2 if '-272189-p00' in e.attrib['CONTENTIDS']]
    assert len(urns) == (n_valid_urns + 1)


def test_urn_granular_fragement_page_k3_newspaper_issue(tmp_path):
    """
    ensure that Kitodo3 Newspaper issue gets
    granular URNs in expected order, i.e.:
    first page = [Seite 1]
    last page  = [Colorchecker]
    """

    # arrange
    urn_main = 'urn:nbn:de:gbv:3:3-171133730-30089663818490701-14'
    src_meta = TEST_RES / "k3_300896638-18490701.xml"
    path_meta = tmp_path / "300896638-18490701.mets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    xml_root = ET.parse(str(path_meta)).getroot()

    # act
    (xml_root_result, _type, rep) = enrich_urn_granular(xml_root, urn_main)
    write_xml_file(xml_root_result, str(path_meta))

    # assert
    assert os.path.exists(str(path_meta))
    assert len(rep.added) == 5
    assert _type == URNType.GRANULAR_FRAGMENT_PAGE
    meta_xml = ET.parse(str(path_meta)).getroot()
    fst_page = meta_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@CONTENTIDS]', XMLNS)[0]
    assert fst_page.attrib['CONTENTIDS'].endswith("30089663818490701-14/fragment/page=0001")
    assert fst_page.attrib['ORDERLABEL'] == '[Seite 1]'
    lst_page = meta_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@CONTENTIDS]', XMLNS)[-1]
    assert lst_page.attrib['CONTENTIDS'].endswith("30089663818490701-14/fragment/page=0005")
    assert lst_page.attrib['ORDERLABEL'] == '[Colorchecker]'


def test_urn_granular_inhouse_zd_replacement_not_enforced(tmp_path):
    """
    ensure existing granular urns are
    kept if no replacement enforced
    """

    # arrange
    urn_main = 'urn:nbn:de:gbv:3:3-62299-19030423018'
    src_meta = TEST_RES / "vls_digital_3014754.zmets.xml"
    path_meta = tmp_path / "3014754.mets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    _reader = MetsReader(path_meta)
    ident_urn = ET.SubElement(_reader.primary_dmd, '{http://www.loc.gov/mods/v3}identifier', {'type': 'urn'})
    ident_urn.text = urn_main
    _reader.write()
    xml_root = ET.parse(str(path_meta)).getroot()

    # act
    (xml_root_result, _type, rep) = enrich_urn_granular(xml_root, urn_main)
    write_xml_file(xml_root_result, str(path_meta))

    # assert
    assert os.path.exists(str(path_meta))
    assert len(rep.added) == 0
    assert len(rep.existed) == 4
    assert _type == URNType.GRANULAR_PAGE
    meta_xml = ET.parse(str(path_meta)).getroot()
    page1 = meta_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@CONTENTIDS]', XMLNS)[0]
    assert "urn:nbn:de:gbv:3:3-62299-p0005-5" == page1.attrib['CONTENTIDS']


def test_urn_granular_inhouse_zd_replacement_enforced(tmp_path):
    """
    ensure existing granular urns can be exchanged
    if re-creation enforced
    """

    # arrange
    urn_main = 'urn:nbn:de:gbv:3:3-62299-19030423018'
    src_meta = TEST_RES / "vls_digital_3014754.zmets.xml"
    path_meta = tmp_path / "3014754.mets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    _reader = MetsReader(path_meta)
    ident_urn = ET.SubElement(_reader.primary_dmd, '{http://www.loc.gov/mods/v3}identifier', {'type': 'urn'})
    ident_urn.text = urn_main
    _reader.write()
    xml_root = ET.parse(str(path_meta)).getroot()

    # act
    (xml_root_result, _type, rep) = enrich_urn_granular(xml_root, urn_main=urn_main, do_replace=True)
    write_xml_file(xml_root_result, str(path_meta))

    # assert
    assert os.path.exists(str(path_meta))
    assert len(rep.added) == 4
    assert len(rep.replaced) == 4
    assert _type == URNType.GRANULAR_FRAGMENT_PAGE
    meta_xml = ET.parse(str(path_meta)).getroot()
    page1 = meta_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@CONTENTIDS]', XMLNS)[0]
    assert "urn:nbn:de:gbv:3:3-62299-19030423018/fragment/page=0001" == page1.attrib['CONTENTIDS']


def test_urn_granular_zdp_replace_granular_urn(tmp_path):
    """
    Strange constellation: pages got granular URN related to
    newspaper's year, but the issues with the page got no
    particular granular URN themselves

    Issue: 1840-12-31

    PLEASE NOTE:
        Since the existing granular URN do *not* fit the
        expected URN pattern, there's actual no replacement
        taking place, therefore do_replace param can
        be ommitted
    """

    # arrange
    urn_main = 'urn:nbn:de:gbv:3:1-1823278450-18401231'
    src_meta = TEST_RES / "vls_digitale_9633116.zmets.xml"
    path_meta = tmp_path / "9633116.zmets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    _reader = MetsReader(path_meta)
    ident_urn = ET.SubElement(_reader.primary_dmd, '{http://www.loc.gov/mods/v3}identifier', {'type': 'urn'})
    ident_urn.text = urn_main
    _reader.write()
    xml_root = ET.parse(str(path_meta)).getroot()

    # act
    (xml_root_result, _type, rep) = enrich_urn_granular(xml_root, urn_main=urn_main, do_replace=True)
    write_xml_file(xml_root_result, str(path_meta))

    # assert
    assert os.path.exists(str(path_meta))
    assert len(rep.added) == 6
    assert len(rep.existed) == 6
    assert rep.existed[0].urn == 'urn:nbn:de:gbv:3:1-694468/fragment/page=9293732'
    assert rep.added[0].urn == 'urn:nbn:de:gbv:3:1-1823278450-18401231/fragment/page=0001'
    assert len(rep.replaced) == 6
    assert rep.replaced['phys9293732'] == 'urn:nbn:de:gbv:3:1-694468/fragment/page=9293732 => urn:nbn:de:gbv:3:1-1823278450-18401231/fragment/page=0001'
    assert rep.existed[0].id == rep.added[0].id
    assert _type == URNType.GRANULAR_FRAGMENT_PAGE
    meta_xml = ET.parse(str(path_meta)).getroot()
    page1 = meta_xml.findall(
        './/mets:structMap[@TYPE="PHYSICAL"]//mets:div[@CONTENTIDS]', XMLNS)[0]
    assert "urn:nbn:de:gbv:3:1-1823278450-18401231/fragment/page=0001" == page1.attrib['CONTENTIDS']


def test_get_identifiers_from_elder_datasets(tmp_path):
    """
    Ensure working impl even on elder data sets
    """

    # arrange
    src_meta = TEST_RES / "vls_digital_737429.mets.xml"
    path_meta = tmp_path / "737429.mets.xml"
    shutil.copyfile(src_meta, str(path_meta))
    _reader = MetsReader(path_meta, dmd_id='md737429')

    # act
    report = _reader.report

    # assert
    assert report.system_identifiers == {'digital.bibliothek.uni-halle.de/hd': '737429'}
