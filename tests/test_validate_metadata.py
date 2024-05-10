"""specification for metadata validation"""

import os
import shutil

import pytest

import lxml.etree as ET

import digiflow as df
import digiflow.validate as dfv
import digiflow.digiflow_metadata as dfmd

from .conftest import TEST_RES

_EXPORT_METS = 'export_mets.xml'


@pytest.mark.skipif("sys.version_info < (3,6)")
def test_validate_archivable_01(tmp_path):
    """Test depends on PosixPath, only works with 3.6+"""

    src_path = TEST_RES / "k2_mets_vd18_147638674.xml"
    path_meta = tmp_path / "147638674.xml"
    shutil.copyfile(src_path, path_meta)

    # act
    assert dfv.validate_xml(path_meta)


def test_create_export_mets_invalid():
    """
    xml validation of some artificial zkw export
    """

    # arrange
    path_zk_export = os.path.join(TEST_RES, 'zkw', '584', _EXPORT_METS)
    assert os.path.exists(path_zk_export)

    # act
    with pytest.raises(dfv.InvalidXMLException) as err:
        dfv.validate_xml(path_zk_export)

    assert len(err.value.args) == 1
    _err = err.value.args[0]
    assert 'ERROR' == _err[0][0]
    assert 'messageDigestAlgorithm' in str(_err)
    assert "value 'foo' is not an element of the set" in str(_err)


def test_create_export_mets_valid():
    """
    xml validation if export_mets from kitodo2 ID 1064
    """

    # arrange
    path_zk_export = os.path.join(TEST_RES, 'zkw', '1064', _EXPORT_METS)
    assert os.path.exists(path_zk_export)

    # act
    assert dfv.validate_xml(path_zk_export)


def test_altov4_is_valid():
    """Ensure validity for ALTO V4"""

    # arrange
    path_altov4_737429 = os.path.join(TEST_RES, 'ocr', 'alto', 'FULLTEXT_737438.xml')
    assert os.path.exists(path_altov4_737429)

    # act
    assert dfv.validate_xml(path_altov4_737429)


def test_altov4_from_kraken_serializer_is_valid():
    """Ensure Kraken produces valid ALTO"""

    # arrange
    path_altov4_737429 = os.path.join(
        TEST_RES, 'ocr', 'alto', 'test-kraken-alto4-serialization.xml')
    assert os.path.exists(path_altov4_737429)

    # act
    assert dfv.validate_xml(path_altov4_737429)


def test_mets_from_migration_mvwvd18_cstage_is_invalid():
    """
    Check oai:digitale.bibliothek.uni-halle.de/vd18:9427342
    when dropped filegroup DOWNLOAD - which is the only fileGroup -
    METS is invalid since fileSec *must* contain at least one fileGrp
    """

    # arrange
    path_2910519 = os.path.join(TEST_RES, 'migration', '2910519.fail.xml')
    assert os.path.exists(path_2910519)

    # act
    with pytest.raises(dfv.InvalidXMLException) as exc:
        dfv.validate_xml(path_2910519)

    # assert
    _info = exc.value.args[0]
    assert 'ERROR' == _info[0][0]
    assert 'SCHEMASV' == _info[0][1]
    assert "Element '{http://www.loc.gov/METS/}fileSec': Missing child element(s)." in str(_info)
    assert "Expected is ( {http://www.loc.gov/METS/}fileGrp )" in str(_info)


def test_mets_with_two_invalids(tmp_path):
    """Ensure *all* invalid things properly reported
    record 1981185920_37167 contains invalid data
    => need to recognize several errors at once
    """

    # arrange
    path_37167 = os.path.join(TEST_RES, 'opendata', '1981185920_37167.xml')

    # ensure errors present
    _org_root = ET.parse(path_37167).getroot()
    _wrong_places = _org_root.findall('.//mods:relatedItem/mods:recordIdentifier', df.XMLNS)
    assert len(_wrong_places) == 1
    _wrong_orders = _org_root.findall('.//mods:mods/mods:recordInfo', df.XMLNS)[0][0]
    assert ET.QName(_wrong_orders).localname == 'recordIdentifier'

    _tmp_dst = tmp_path / '37167.xml'
    shutil.copyfile(path_37167, _tmp_dst)
    dfmd.extract_mets(_tmp_dst, open(_tmp_dst, mode='rb').read())

    # act
    with pytest.raises(dfv.InvalidXMLException) as exc:
        dfv.validate_xml(_tmp_dst)

    # assert
    _msg = exc.value.args[0]
    assert len(_msg) == 1
    assert "recordIdentifier': This element is not expected" in str(_msg[0])
    