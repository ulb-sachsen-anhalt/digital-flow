# -*- coding: utf-8 -*-
"""Digiflow validation API"""

import shutil

from pathlib import (
    Path
)

import pytest

from digiflow.digiflow_validate import (
    DDB_IGNORE_RULES_MVW,
    FAILED_ASSERT_ERROR,
    FAILED_ASSERT_OTHER,
    DigiflowDDBException,
    ddb_validation,
)

from .conftest import (
    TEST_RES
)

# switch off saxon if not available
SAXON_PY_ENABLED = True
try:
    import saxonche
except ModuleNotFoundError: 
    SAXON_PY_ENABLED = False


@pytest.fixture(name="share_it_monography")
def _fixture_share_it_monography(tmp_path):
    """Provide test fixture"""

    testroot = tmp_path / 'VALIDATIONTEST'
    testroot.mkdir()
    file_name = '1981185920_44046.xml'
    mets_source = Path(TEST_RES) / 'ocr' / file_name
    mets_target = Path(str(testroot), file_name)
    shutil.copy(str(mets_source), str(mets_target))
    return str(mets_target)


def test_ddb_validate_opendata_44046_defaults(share_it_monography):
    """Schematron validation with common monography (Aa)
    and default ignorance yield no problems
    """

    # act
    outcome = ddb_validation(share_it_monography)

    # assert
    assert len(outcome) == 0    # nothing invalid to see


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_opendata_44046_ignore_ident01(share_it_monography):
    """Suppress identifier validation message for 
    simple monography (Aa)"""

    # act
    outcome = ddb_validation(share_it_monography, ignore_rules=['identifier_01'])

    # assert
    assert 'info' not in outcome
    assert len(outcome) == 0


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_opendata_44046_without_ignorances(share_it_monography):
    """XSLT validation with common monography (Aa)
    complains about identifier type 'gbv'
    """

    # act
    outcome = ddb_validation(share_it_monography, ignore_rules=[])

    # assert
    assert len(outcome) == 1
    assert 'identifier_01' in outcome[FAILED_ASSERT_OTHER][0]
    assert 'type:gbv' in outcome[FAILED_ASSERT_OTHER][0]
    assert '265982944' in outcome[FAILED_ASSERT_OTHER][0]


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_legacy_monography_raw(tmp_path):
    """XSLT validation outcome for out-dated
    print export data from Kitodo2 legacy system
    
    Please note, that any other roles like info or warning
    (=> amdSec_13, amdSec_15) will be swalled
    """

    # arrange
    file_name = 'k2_mets_vd18_147638674.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target)

    # assert
    _exc_load = _dexc.value.args[0]
    assert len(_exc_load) == 2
    assert isinstance(_exc_load, list)
    # changed from 05/22 => 06/23
    # from location_02 => location_01
    assert 'location_01' in _exc_load[0]
    assert 'dmdSec_04' in _exc_load[1]

@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_legacy_monography_curated(tmp_path):
    """DDB compliant data with curated METS which doesn't 
    contain former additional dmdSec anymore so only
    warnings concerning licence and digiprovMD remain
    """

    # arrange
    file_name = 'k2_mets_vd18_147638674_curated.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_validation(mets_target)

    # assert 
    assert len(result) == 1
    assert FAILED_ASSERT_ERROR not in result
    assert len(result[FAILED_ASSERT_OTHER]) == 2 
    assert 'amdSec_13' in result[FAILED_ASSERT_OTHER][0]


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_menadoc_44080924x(tmp_path):
    """XSLT validation outcome for rather
    recent digitized object from menadoc retro-digi
    
    changes between 05/22 and 06/23
    * increase number of errors from 3 to 16(!)
    * assert 'originInfo_06' in result[DDB_ERROR][0]
    * assert 'location_02' in result[DDB_ERROR][1]
    * assert 'dmdSec_04' in result[DDB_ERROR][2]
    """

    # arrange
    file_name = 'k2_mets_mena_44080924X.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target)

    # assert
    _load = _dexc.value.args[0]
    assert len(_load) == 16
    assert 'originInfo_06' in _load[0]
    assert 'location_01' in _load[1] # before: location_02
    assert 'dmdSec_04' in _load[2]


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142537_raw(tmp_path):
    """XSLT validation outcome for VD18 c-stage
    without default customm ignore rules
    
    changed from 05/22 => 06/23
    * increased errors from 7 to 11
    """

    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target, ignore_rules=[])

    # assert 
    _load = _dexc.value.args[0]
    assert len(_load) == 11
    assert 'titleInfo_02' in _load[0]
    assert 'originInfo_06' in _load[1]
    assert 'originInfo_06' in _load[2]
    assert 'location_01' in _load[3]
    assert 'dmdSec_04' in _load[4]
    assert 'fileSec_02' in _load[5]
    assert 'structMapLogical_17' in _load[6]

@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142537_dafault_ignorances(tmp_path):
    """Schematron validation outcome for VD18 F-Stage
    which considers default ignore rules for
    multivolumes like LOCTYPE HREF and alike or
    wants fileGroup@USE='DEFAULT' (which isn't present yet)
    """

    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target, ignore_rules=DDB_IGNORE_RULES_MVW)

    # assert 
    _load = _dexc.value.args[0]
    assert len(_load) == 4
    assert _load[0].startswith('[originInfo_06]')
    assert _load[1].startswith('[originInfo_06]')
    assert _load[2].startswith('[location_01')
    assert _load[3].startswith('[dmdSec_04]')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142537_curated(tmp_path):
    """DDB compliant with curated METS which doesn't contain
    the former additional dmdSec anymore and also ignores
    originInfo_06 issues"""

    # arrange
    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    _ignore_them = DDB_IGNORE_RULES_MVW + ['originInfo_06']
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target, ignore_rules=_ignore_them)

    # assert 
    _load = _dexc.value.args[0]
    assert len(_load) == 2
    assert _load[0].startswith('[location_01')
    assert _load[1].startswith('[dmdSec_04]')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142340_raw(tmp_path):
    """Schematron validation outcome for VD18 C-Stage
    corresponding to k2_mets_153142537.xml as-it-is"""

    # arrange
    file_name = 'k2_mets_vd18_153142340.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target, ignore_rules=[])

    # assert 
    _load = _dexc.value.args[0]
    assert len(_load) == 3
    assert _load[0].startswith('[originInfo_06]')
    assert _load[1].startswith('[originInfo_06]')
    assert _load[2].startswith('[structMapLogical_17]')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142340( tmp_path):
    """Schematron validation outcome for VD18 C-Stage
    corresponding to k2_mets_153142537.xml
    considering ULB default ignore rules"""

    # arrange
    file_name = 'k2_mets_vd18_153142340.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target, digi_type='Ac')

     # assert 
    _load = _dexc.value.args[0]
    assert len(_load) == 2
    assert _load[0].startswith('[originInfo_06]')
    assert _load[1].startswith('[originInfo_06]')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_morbio_1748529021(tmp_path):
    """Schematron validation outcome for recent Morbio
    export considering ULB default ignore rules
    
    changed due 2023/06: dropped failure for amdSec_05 (licence)
    """

    # arrange
    file_name = 'k2_mets_morbio_1748529021.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target, digi_type='Ac')

    # assert 
    _load = _dexc.value.args[0]
    assert len(_load) ==  2    # 2 errors
    assert _load[0].startswith('[location_01]')
    assert _load[1].startswith('[dmdSec_04]') # suspicious additional dmdSec


def test_ddb_validate_newspaper(tmp_path):
    """Test recent DDB-Validation 06/23 for
    migrated newspaper issue missing
    * license information
    """

    # arrange
    the_name = 'vls_digital_3014754.zmets.xml'
    mets_source = Path(TEST_RES) / the_name
    mets_target = Path(str(tmp_path), the_name)
    shutil.copy(str(mets_source), str(mets_target))

     # act
    with pytest.raises(DigiflowDDBException) as _dexc:
        ddb_validation(mets_target, digi_type='issue')

    # assert
    _load = _dexc.value.args[0]
    assert len(_load) == 1
    assert '[amdSec_04]' in _load[0]


def test_ddb_validate_newspaper_02(tmp_path):
    """Test recent DDB-Validation 06/23 for
    migrated newspaper issue which is quite o.k.
    """

    # arrange
    the_name = 'zd1-opendata2-1516514412012-59265.xml'
    mets_source = Path(TEST_RES) / 'opendata2' / the_name
    mets_target = Path(str(tmp_path), the_name)
    shutil.copy(str(mets_source), str(mets_target))

     # act
    _result = ddb_validation(mets_target, digi_type='OZ')

    # assert
    assert len(_result) == 0
