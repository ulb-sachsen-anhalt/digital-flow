# -*- coding: utf-8 -*-

import os
import shutil

from pathlib import Path

import pytest

import lxml.etree as ET

import digiflow as df
import digiflow.common as dfc
import digiflow.validate as dfv

from .conftest import TEST_RES, LIB_RES



def test_metsreader_kitodo2_mvw():
    """
    Correct primary mods?
    """

    # arrange
    path = os.path.join(TEST_RES, 'k2_mets_vd18_183475917.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path)

    # act
    report = reader.analyze()

    # assert
    assert report.languages == ['ger']
    assert not report.images
    assert report.type == 'Af'
    assert 'gvk-ppn' in report.identifiers
    assert report.identifiers['gvk-ppn'] == '183475917'
    assert report.hierarchy == [('DMDLOG_0001', 'volume'), ('183475631', 'multivolume_work')]
    assert report.locations == ['Nr 83 (6)']


def test_metsreader_report_vd18_cstage():
    """
    Are correct informations extracted for c-stage?
    """

    # arrange
    path = os.path.join(TEST_RES, 'migration', '9427342.mets.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path, 'md9427342')

    # act
    report = reader.analyze()

    # assert
    assert report.languages == ['ger']
    assert not report.images
    assert report.type == 'Ac'
    assert report.identifiers['ulbhalvd18'] == '211999504'
    assert report.hierarchy == [('9427342', 'multivolume_work')]
    assert not report.locations


def test_metsreader_report_vd18_fstage():
    """
    Correct informations extracted for f-stage?
    """

    # arrange
    path = os.path.join(TEST_RES, 'migration', '9427337.mets.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path, 'md9427337')

    # act
    report = reader.analyze()

    # assert
    assert report.languages == ['ger']
    assert not report.images
    assert report.type == 'Af'
    assert report.identifiers['ulbhalvd18'] == '211999628'
    assert report.hierarchy == [
        ('9427337', 'volume'), ('9427342', 'multivolume_work')]
    assert report.locations == ['Lb 712 a (3,2)']


def test_metsreader_report_vd17_fstage_pica_case():
    """
    Correct informations extracted for VD17 F-stage?
    Is the second letter from PICA preserved?
    """

    # arrange
    path = os.path.join(TEST_RES,
                        'migration',
                        'vd17-14591176.mets.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path, 'md14591176')

    # act
    report = reader.analyze()

    # assert
    assert report.languages == ['ger']
    assert not report.images
    assert report.type == 'AF'
    assert report.identifiers['pon'] == '008499756'
    assert report.hierarchy == [
        ('14591176', 'volume'), ('14591136', 'multivolume_work')]
    assert report.locations == ['TM0904 (5)']


def test_metsreader_report_hd_monography():
    """
    Correct extractions for hd monography?
    """

    # arrange
    path = os.path.join(TEST_RES,
                        'vls',
                        'monography',
                        '10595.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path, 'md10595')

    # act
    report = reader.analyze()

    # assert
    assert report.languages == ['ger']
    assert not report.images
    assert report.type == 'Aa'
    assert report.identifiers['ulbhaldod'] == '187143188'
    assert report.hierarchy == [('10595', 'monograph')]
    assert len(report.locations) == 2
    assert report.locations == ['Pon IIg 694, FK', 'Pon IIg 689, 4° (2)']


def test_metsreader_report_kitodo2_export_monography():
    """
    Correct extractions for hd monography?
    """

    # arrange
    path = os.path.join(TEST_RES, 'k2_mets_vd18_147638674.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path, 'DMDLOG_0000')

    # act
    report = reader.analyze()
    # need to set this manually since
    # we do not know the kitodo ID from METS
    report.system_identifiers[df.MARK_KITODO2] = '1234'

    # assert
    assert report.languages == ['ger']
    assert not report.images
    assert report.type == 'Aa'
    assert report.identifiers['gvk-ppn'] == '147638674'
    assert len(report.identifiers) == 3
    assert report.system_identifiers == {'kitodo2' : '1234'}
    assert report.hierarchy == [('DMDLOG_0000', 'monograph')]
    assert report.locations == ['Pon Za 5950, QK']


def test_metsreader_logical_type_1686755_is_document():
    """
    Check digital object 1686755
    * logical type is 'document'
    * no external PICA-type present (=None)
    """

    # arrange
    mets = os.path.join(TEST_RES, 'migration/1686755.oai.xml')

    # arrange
    mets_reader = df.MetsReader(mets, 'md1686755')

    # assert
    outcome = mets_reader.get_type_and_hierarchy()
    assert outcome == (None, 'document', [('1686755', 'document')])


def test_metsreader_type_pica_monography():
    """This item has proper type from pica annotation"""

    # arrange
    mets = os.path.join(TEST_RES, 'migration/201517.oai.xml')

    # act
    mets_reader = df.MetsReader(mets, 'md201517')

    assert "Aa" in mets_reader.get_type_and_hierarchy()


def test_metsreader_wrong_logical_type():
    """
    What happens when encountered an invalid c-stage oai-response?
    Extended Test due data errors from OCR-D-Pilotproject
    => logical type monograph should be multivolume
    """

    # arrange
    mets = os.path.join(TEST_RES, 'migration/416811.mets.xml')
    mets_reader = df.MetsReader(mets, 'md416811')

    # has no picaType enriched
    # asserts "monograph", which is actually *wrong*
    # in this case it should be "multivolume_work"
    # f-stage 424336
    # f-stage 415691
    outcome = mets_reader.get_type_and_hierarchy()
    assert outcome == (None, 'monograph', [('416811', 'monograph')])


@pytest.fixture(name="monograph_hd_invalid_physlinks")
def _fixture_monograph_hd_10595(tmpdir):

    target_file = str(tmpdir.mkdir('vlmetadata').join('10595.mets.xml'))
    source_file = os.path.join(TEST_RES, 'migration/10595.mets.xml')
    shutil.copy(source_file, target_file)
    return str(target_file)


def test_metsreader_invalid_physlinks_10595(monograph_hd_invalid_physlinks):
    """
    Handle digital objects with invalid links from
    logical maps to physical structure
    """
    mets = df.MetsReader(monograph_hd_invalid_physlinks, 'md10595')
    structs = mets.get_invalid_physical_structs()

    assert len(structs) == 26
    assert structs[0] == ('log10601', 'phys2376732', 'phys10603')
    assert structs[14] == ('log2376740', 'phys2376742', 'phys10604')
    assert structs[15] == ('log2376737', 'phys2376739', 'phys10604')
    assert structs[16] == ('log2376734', 'phys2376736', 'phys10604')


def test_metsreader_report_for_10595(monograph_hd_invalid_physlinks):
    """Report for digital library object hd/10595"""

    # arrange
    reader = df.MetsReader(monograph_hd_invalid_physlinks, 'md10595')

    # act
    report = reader.analyze()

    # assert
    assert 'Aa' in report.type
    assert report.languages == ['ger']
    assert report.identifiers['ulbhaldod'] == '187143188'
    assert len(report.links) == 26


def test_metsreader_logical_type_is_multivolume():
    """
    Check that a multivolume_work with 4 volumes is
    correct recognized as pica 'Ac' and 'multivolume_work'
    """

    # arrange
    mets = os.path.join(TEST_RES, 'migration/9427342.mets.xml')
    mets_reader = df.MetsReader(mets, 'md9427342')

    # act
    mets_reader.analyze()

    # assert
    outcome = mets_reader.get_type_and_hierarchy()
    assert outcome == ('Ac', 'multivolume_work', [
                       ('9427342', 'multivolume_work')])


def test_metsreader_logical_type_is_tome():
    """
    Check that a part of a multivolume_work, 9427334,
    is VL 'tome' and pica 'Af'
    """

    # arrange
    mets = os.path.join(TEST_RES, 'migration/9427334.mets.xml')
    mets_reader = df.MetsReader(mets, 'md9427334')

    # act
    mets_reader.analyze()

    # assert
    outcome = mets_reader.get_type_and_hierarchy()
    assert outcome == (
        'Af', 'tome', [('9427334', 'tome'), ('9427342', 'multivolume_work')])


def test_metsreader_ambigious_recordinfo():
    """
    Handle strange cornercase in HD collection where digital objects
    posses several recordInfo elements but first match is empty
    """

    # arrange
    mets = os.path.join(TEST_RES, 'migration/369765.mets.xml')
    mets_reader = df.MetsReader(mets, 'md369765')

    # act
    mets_reader.analyze()

    # assert
    outcome = mets_reader.get_type_and_hierarchy()
    assert outcome == (None, 'monograph', [('369765', 'monograph')])


def test_metsreader_clear_agents(tmp_path):
    """
    Handle strange cornercase in HD collection where digital objects
    posses several recordInfo elements but th first match is empty
    """

    # arrange
    mets = os.path.join(TEST_RES, 'migration/369765.mets.xml')
    the_orig = ET.parse(mets)
    orig_agents = the_orig.findall('.//mets:agent', dfc.XMLNS)
    assert len(orig_agents) == 4
    dst = tmp_path / '369765.mets.xml'
    shutil.copyfile(mets, str(dst))
    mets_reader = df.MetsReader(str(dst), 'md369765')

    # act
    mets_reader.clear_agents('OTHERTYPE', ['REPOSITORY', 'INSTANCE'])
    result_path = mets_reader.write('ulb')

    # assert
    the_root = ET.parse(str(result_path))
    agents = the_root.findall('.//mets:agent', dfc.XMLNS)
    assert len(agents) == 2
    for agent in agents:
        assert 'REPOSITORY' not in agent.attrib['OTHERTYPE']
        assert 'INSTANCE' not in agent.attrib['OTHERTYPE']


def test_metsreader_enrich_first_agent(tmp_path):
    """
    Ensure mets:agent doesn't yield invalid METS =>
    insert agent at right position
    cf. https://www.loc.gov/standards/mets/mets.xsd

    Prevent
    digiflow.validate.metadata_xsd.InvalidXMLException: 
        [('ERROR', 
          'SCHEMASV', 
          "Element '{http://www.loc.gov/METS/}agent': This element is not expected.")]
    """

    # arrange
    mets = os.path.join(TEST_RES, 'k3_300896638-18490701.xml')
    mets_input = ET.parse(mets)
    orig_agents = mets_input.findall('.//mets:agent', dfc.XMLNS)
    assert len(orig_agents) == 0
    dst = tmp_path / 'mets.xml'
    shutil.copyfile(mets, str(dst))
    mets_reader = df.MetsReader(dst)

    # act
    mets_reader.enrich_agent('Agent Smith')
    result_path = mets_reader.write('ulb')

    # assert
    assert Path(result_path).exists()
    dfv.validate_xml(ET.parse(result_path).getroot()) # no Exception plz

def test_metsreader_enrich_another_agent(tmp_path):
    """
    Ensure mets:agent doesn't yield invalid METS =>
    insert agent at right position
    cf. https://www.loc.gov/standards/mets/mets.xsd

    Prevent
    digiflow.validate.metadata_xsd.InvalidXMLException: 
        [('ERROR', 
          'SCHEMASV', 
          "Element '{http://www.loc.gov/METS/}agent': This element is not expected.")]
    """

    # arrange
    mets = os.path.join(TEST_RES, 'k3_300896638-18490701.xml')
    mets_input = ET.parse(mets)
    orig_agents = mets_input.findall('.//mets:agent', dfc.XMLNS)
    assert len(orig_agents) == 0
    dst = tmp_path / 'mets.xml'
    shutil.copyfile(mets, str(dst))
    mets_reader = df.MetsReader(dst)

    # act
    mets_reader.enrich_agent('Agent J')
    mets_reader.enrich_agent('Agent K')
    result_path = mets_reader.write('ulb')

    # assert
    assert Path(result_path).exists()
    dfv.validate_xml(ET.parse(result_path).getroot()) # no Exception plz


def test_metsreader_enrich_agent_kwargs(tmp_path):
    """
    Ensure mets:agent doesn't yield invalid METS =>
    insert agent at right position
    cf. https://www.loc.gov/standards/mets/mets.xsd

    Prevent
    digiflow.validate.metadata_xsd.InvalidXMLException: 
        [('ERROR', 
          'SCHEMASV', 
          "Element '{http://www.loc.gov/METS/}agent': This element is not expected.")]
    """

    # arrange
    mets = os.path.join(TEST_RES, 'k3_300896638-18490701.xml')
    mets_input = ET.parse(mets)
    orig_agents = mets_input.findall('.//mets:agent', dfc.XMLNS)
    assert len(orig_agents) == 0
    dst = tmp_path / '369765.mets.xml'
    shutil.copyfile(mets, str(dst))
    mets_reader = df.MetsReader(dst)

    # act
    kwargs = {'ROLE': 'CREATOR', 'TYPE': 'INDIVIDUAL'}
    mets_reader.enrich_agent(agent_name='Agent Smith', **kwargs)
    result_path = mets_reader.write('ulb')

    # assert
    assert Path(result_path).exists()
    result_root = ET.parse(result_path).getroot()
    dfv.validate_xml(result_root) # no Exception plz
    assert result_root.xpath('.//mets:agent[@TYPE="INDIVIDUAL"]/mets:name/text()',
                             namespaces=df.XMLNS)[0] == 'Agent Smith'


def test_metsreader_log_hierarchy_menadoc_oai_record_section():
    """
    Handle strange records from menadoc with logical root element
    of type section/article which refers to a MODS section
    without any identifiers from catalogues/databases/urn
    """

    # arrange
    target_file = os.path.join(TEST_RES, 'migration/20586.mets.xml')

    # act
    _mreader = df.MetsReader(target_file)
    with pytest.raises(RuntimeError) as rer:
        _report = _mreader.report
    assert 'no record _identifiers' in rer.value.args[0]

    # assert - no, this is not happening!
    #assert _report.identifiers['menadoc.bibliothek.uni-halle.de/dmg'] == '20586'


def test_metsreader_opendata2_inspect_migrated_record_identifiers():
    """Inspect migrated OAI record identifiers
    from digitale.bibliothek.uni-halle.de/vd16 to
    opendata2.uni-halle.de

    Please note: for the first migrated records
    the legacy identifier format was subject to
    change, therefore the mappings must be set
    outside the METS-data
    """

    # arrange
    target_file = os.path.join(TEST_RES, 'mets/vd16_opendata2_1516514412012_4400.xml')
    mets_reader = df.MetsReader(target_file)

    # act
    report = mets_reader.analyze()

    # assert
    assert mets_reader.dmd_id == 'md998423'
    assert len(report.identifiers) == 5
    assert report.identifiers == {
        'urn': 'urn:nbn:de:gbv:3:1-507459',
        'vd16': 'ZV 932',
        'bvb': 'VD0034491',
        'gvk-ppn': '567526844',
        'doi': 'doi:10.25673/opendata2-4398'
    }
    assert report.system_identifiers == {
        'legacy vlid': '998423',
        'opendata2.uni-halle.de': 'https://opendata2.uni-halle.de//handle/1516514412012/4400',
    }


def test_metsreader_opendata_migrated_record_with_doi():
    """Ensure migrated data finally also contains
    a DOI identifier which was registered afterwards"""

    # arrange
    target_file = os.path.join(TEST_RES, 'opendata/1981185920_43053.xml')
    mets_reader = df.MetsReader(target_file)

    # act
    report = mets_reader.analyze()

    # assert
    assert mets_reader.dmd_id == 'md1177525'
    assert len(report.identifiers) == 5
    assert report.identifiers == {
        'urn': 'urn:nbn:de:gbv:3:1-132151',
        'gvk-ppn': '216311322',
        'doi': 'doi:10.25673/41099',
        'gbv': '216311322',
        'vd18': '10078320',
    }
    assert report.system_identifiers == {
        'legacy vlid': '1177525',
        'opendata.uni-halle.de': 'https://opendata.uni-halle.de//handle/1981185920/43053',
    }


def test_metsreader_opendata_inspect_migrated_record_origins():
    """How to handle migrate OAI records"""

    # arrange
    target_file = os.path.join(TEST_RES, 'opendata2/vd16-opendata2-1516514412012-4400.xml')
    mets_reader = df.MetsReader(target_file)

    # act
    report = mets_reader.analyze()

    # 3 origins, which is of course wrong
    assert report.origins == [('n.a.', 'Freiberg'), ('n.a.', 'Freiberg'),
                              ('digitization', 'Halle (Saale)')]


def test_metsreader_zd1_issue_16767392():
    """Check METS-Reader-output"""

    # arrange
    mets = os.path.join(TEST_RES, 'vls/zd/zd1-16767392.oai.xml')

    # act
    mets_reader = df.MetsReader(mets, 'md16767392')

    assert "16767392" in mets_reader.get_identifiers()['local']
    assert "issue" in mets_reader.get_type_and_hierarchy()


def test_metsreader_zd1_issue_16359609():
    """Check METS-Reader-output"""

    # arrange
    mets = os.path.join(TEST_RES, 'vls/zd/zd1-16359609.mets.xml')

    # act
    mets_reader = df.MetsReader(mets)

    assert "ulbhalvd:16359609" == mets_reader.get_identifiers()['local']
    assert "issue" in mets_reader.get_type_and_hierarchy()


def test_metsprocessor_clear_filegroups_migration_vd17(tmp_path):
    """Check MetsProcessor"""

    # arrange
    mets = os.path.join(TEST_RES, 'migration/vd17-14591176.mets.xml')
    the_orig = ET.parse(mets)
    orig_file_groups = the_orig.findall('.//mets:fileGrp', dfc.XMLNS)
    assert len(orig_file_groups) == 6

    dst = tmp_path / '14591176.xml'
    shutil.copyfile(mets, str(dst))
    mets_proc = df.MetsProcessor(str(dst), '14591176')

    # act
    mets_proc.clear_filegroups(black_list=['TEASER', 'DOWNLOAD', 'DEFAULT', 'THUMBS', 'MIN'])
    mets_proc.write()
    new_tree = ET.parse(str(dst))
    assert len(new_tree.findall('.//mets:fileGrp', dfc.XMLNS)) == 1
    dfv.validate_xml(new_tree.getroot())


def test_metsprocessor_clear_filegroups_odem_ocrd(tmp_path):
    """MetsProcessor with already ocr-ed VD18 print"""

    # arrange
    mets = os.path.join(TEST_RES, 'opendata/1981185920_38841.xml')
    the_orig = ET.parse(mets)
    orig_file_groups = the_orig.findall('.//mets:fileGrp', dfc.XMLNS)
    assert len(orig_file_groups) == 5

    dst = tmp_path / '1981185920_38841.xml'
    shutil.copyfile(mets, str(dst))
    mets_proc = df.MetsProcessor(str(dst))

    # act
    mets_proc.clear_filegroups(black_list=['DOWNLOAD', 'DEFAULT', 'THUMBS', 'FULLTEXT'])
    mets_proc.write()
    
    # assert
    new_tree = ET.parse(dst)
    dfv.validate_xml(new_tree.getroot())
    assert len(new_tree.findall('.//mets:fileGrp', dfc.XMLNS)) == 1


def test_metsreader_zd2_issue_18680621():
    """Check METS-Reader with new kitodo3 newspaper structure"""

    # arrange
    mets = os.path.join(TEST_RES, 'kitodo3-zd2/1021634069-18680621.xml')

    # act
    mets_reader = df.MetsReader(mets)

    _idents = mets_reader.get_identifiers()
    assert _idents == {
        'urn': 'urn:nbn:de:gbv:3:1-171133730-102163406918680621-11',
        'kxp-ppn': '102163406918680621',
    }
    assert mets_reader._system_identifiers() == {'kitodo3': '4583'}
    _pica, _type, _tree = mets_reader.get_type_and_hierarchy()
    assert _pica == 'AB'
    assert _type == 'issue'
    assert _tree == [('uuid-bb586c88-d9b1-3cef-9a91-4e9f4fa5cc8a', 'issue'),
                     ('n.a.', 'day'),
                     ('n.a.', 'month'),
                     ('1021634069_1868', 'year'),
                     ('1021634069', 'newspaper')]


@pytest.mark.parametrize(["mets_path", "dmd_id"], [
    (os.path.join(TEST_RES, 'migration/10595.mets.xml'), 'md10595'),
    (os.path.join(TEST_RES, 'migration/9427342.mets.xml'), 'md9427342'),
    (os.path.join(TEST_RES, 'migration/9427337.mets.xml'), 'md9427337'),
    (os.path.join(TEST_RES, 'k2_mets_vd18_058141367.xml'), 'DMDLOG_0000'),
    (os.path.join(TEST_RES, 'k2_mets_vd18_147638674.xml'), 'DMDLOG_0000'),
    (os.path.join(TEST_RES, 'vls/zd/zd1-16359609.mets.xml'), 'md16359609'),
    (os.path.join(TEST_RES, 'opendata/123456789_27949.xml'), 'md1180329'),
])
def test_metsreader_identify_prime_dmd_section(mets_path, dmd_id):

    # act
    mets_reader = df.MetsReader(mets_path)

    # assert
    assert mets_reader._prime_mods_id == dmd_id


def test_metsprocessor_remove_elements_and_close_tags(tmp_path):
    """Ensure: by removing elements, markup like

        <String ID="word1" CONTENT="hellu">
            </String>

        is prohibited
    """

    # arrange
    path_altov4_737429 = os.path.join(TEST_RES, 'ocr', 'alto', 'FULLTEXT_737438.xml')
    dst = tmp_path / '737438.xml'
    shutil.copyfile(path_altov4_737429, str(dst))
    mets_proc = df.MetsProcessor(str(dst))

    # act
    mets_proc.remove(['alto:Shape', 'alto:Processing'])
    path_result = mets_proc.write()

    # assert
    the_lines = [l.strip() for l in open(path_result, encoding='utf-8').readlines()]
    for _line in the_lines:
        assert '</String>' not in _line


def test_metsprocessor_remove_elements_no_keyerror(tmp_path):
    """Ensure legacy namespaces don't cause trouble anymore
    
    Test Target: Prevent regression
    """

    # arrange
    _a_path = os.path.join(TEST_RES, 'vls_menadoc_99454.mets.xml')
    dst = tmp_path / '99454.xml'
    shutil.copyfile(_a_path, str(dst))
    mets_proc = df.MetsProcessor(str(dst))
    assert len(mets_proc.xpath('//vl:sourceinfo')) == 1

    # act
    mets_proc.remove(['vl:sourceinfo'])
    path_result = mets_proc.write()

    # assert
    resl_proc = df.MetsProcessor(path_result)
    assert len(resl_proc.xpath('//vl:sourceinfo')) == 0


def test_metsreader_logical_hierachy_newspaper_issue():
    """
    Expect the parent hierarchy for single issue
    to be alike this:
    * newspaper 16289661
    * year 17308997
    * month 03
    * day 0322
    * issue 16359603
    """

    # arrange
    mets = os.path.join(TEST_RES, 'vls/zd/zd1-issue-16359603.zmets.xml')
    mets_reader = df.MetsReader(mets, 'md16359603')

    # act
    pica, log_typ, hierachy = mets_reader.get_type_and_hierarchy()

    # assert
    assert not pica
    assert log_typ == 'issue'
    assert hierachy[1:] == [('day0322', 'day'), ('month03', 'month'),
                            ('17308997', 'year'), ('16289661', 'newspaper')]


def test_metsreader_missing_struct_mapping():
    """Check behavior if METS contains logical containers
    without corresponding physical stuff - i.e. an empty
    logical section without any pages which will 
    kill Derivans 1.6.4 afterwards

    => filter em out
    """

    # arrange
    mets = os.path.join(TEST_RES, 'opendata/1981185920_43053.xml')
    mets_reader = df.MetsReader(mets)
    assert mets_reader.dmd_id == 'md1177525'

    # act
    with pytest.raises(RuntimeError) as _runtime_error:
        mets_reader.check()

    # assert
    assert " no link for logical section:'log1646693'" in _runtime_error.value.args[0]


DIGIFLOW_CONFIG = LIB_RES / 'digilife.ini'


def test_metsreader3_report_vd18_cstage():
    """
    Are correct informations extracted for c-stage?
    """

    # arrange
    path = os.path.join(TEST_RES, 'migration', '9427342.mets.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path, 'md9427342')
    reader.config = DIGIFLOW_CONFIG
    _identifier_opts = reader.config.options('identifier')
    assert len(_identifier_opts) == 14

    # act
    _report = reader.report

    # assert
    assert _report.languages == ['ger']
    assert not _report.images
    assert _report.system_identifiers == {'digitale.bibliothek.uni-halle.de/vd18': '9427342'}
    assert _report.identifiers == {'gbv': '211999504',
                                   'ulbhalvd18': '211999504',
                                   'urn': 'urn:nbn:de:gbv:3:1-636051',
                                   'vd18': '11228628'}
    assert _report.type == 'Ac'
    assert _report.hierarchy == [('9427342', 'multivolume_work')]
    assert not _report.locations


def test_metsreader3_report_vd18_fstage():
    """
    Correct informations extracted for f-stage?
    """

    # arrange
    path = os.path.join(TEST_RES, 'migration', '9427337.mets.xml')
    assert os.path.exists(path)
    reader = df.MetsReader(path, 'md9427337')
    reader.config = DIGIFLOW_CONFIG

    # act
    _report = reader.report

    # assert
    assert _report.languages == ['ger']
    assert not _report.images
    assert _report.type == 'Af'
    assert _report.system_identifiers == {
        'digitale.bibliothek.uni-halle.de/vd18': '9427337'  
    }
    assert _report.identifiers == {'gbv': '211999628',
                                   'ulbhalvd18': '211999628',
                                   'urn': 'urn:nbn:de:gbv:3:1-635986',
                                   'vd18': '90311817'}
    assert _report.hierarchy == [
        ('9427337', 'volume'), ('9427342', 'multivolume_work')]
    assert _report.locations == ['Lb 712 a (3,2)']


def test_metsreader_kitodo2_mena_periodical_volume_dmd_id():
    """Process rather unbalanced Kitodo2 MENA volume
    Metadata, i.e. we failed to recognize the prime
    MODS section at first sight
    """

    # arrange
    mets = os.path.join(TEST_RES, 'k2_mets_mena_12988274719564.xml')

    # act
    mets_reader = df.MetsReader(mets)

    # assert
    assert mets_reader.dmd_id == 'DMDLOG_0001'


def test_metadata_processor_contains_single_fgroup():
    """Ensure behavior for passing single str arg
    Bugfix: 
    TypeError: '>' not supported between instances of 'list' and 'int'
    """

    _proc = df.MetsProcessor(TEST_RES / 'k2_mets_morbio_1748529021.xml')
    assert _proc.contains_group('MAX')


def test_metadata_processor_contains_multiple_fgroup():
    """Ensure behavior for passing list args"""

    _proc = df.MetsProcessor(TEST_RES / 'k2_mets_morbio_1748529021.xml')
    assert _proc.contains_group(['MAX'])
