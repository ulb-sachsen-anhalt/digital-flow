# -*- coding: utf-8 -*-

import os
import pytest
import shutil
from pathlib import (
    Path
)
from unittest import (
    mock
)

# switch off saxon py if not available
SAXON_PY_ENABLED = True
try:
    import saxonpy
except ModuleNotFoundError: 
    SAXON_PY_ENABLED = False

from digiflow.digiflow_validate import (
    ddb_xslt_validation,
    ddb_validation,
    DDB_IGNORE_RULES_MVW,
    TMP_SCH_FILE_NAME,
)

from .conftest import (
    TEST_RES
)

ROOT = Path(__file__).parents[1]
PROJECT_ROOT_DIR = Path(__file__).resolve().parents[1]
SCHEMATRON_DIR = PROJECT_ROOT_DIR / 'src' / 'digiflow' / 'schematron'
SCHEMATRON_BIN = str(SCHEMATRON_DIR / 'schxslt-cli.jar')


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


def _place_file(path_dir: Path, mets_file_name: str):
    """Little Helper to push test data"""
    _src_name = f'sch_result_{mets_file_name}'
    _src_path = Path(TEST_RES) / _src_name
    _dst_path = path_dir / TMP_SCH_FILE_NAME
    shutil.copy(str(_src_path), str(_dst_path))
    return str(_dst_path)


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_opendata_44046_default(sch_mock, share_it_monography):
    """Schematron validation with common monography (Aa)
    and nothing to ignore"""

    # arrange
    tmp_path = Path(os.path.dirname(share_it_monography))
    file_name= os.path.basename(share_it_monography)
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    outcome = ddb_validation(share_it_monography)

    # assert
    assert sch_mock.call_count == 1
    assert len(outcome) == 0    # nothing invalid to see


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_opendata_44046_no_ignores(sch_mock, share_it_monography):
    """Schematron validation with common monography (Aa)
    and nothing to ignore"""

    # arrange
    tmp_path = Path(os.path.dirname(share_it_monography))
    file_name= os.path.basename(share_it_monography)
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    outcome = ddb_validation(share_it_monography, ignore_rules=[])

    # assert
    assert sch_mock.call_count == 1
    assert outcome['info'] == [('identifier_01', 'type=gbv')]


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason=f'no saxonpy bindings')
def test_ddb_xslt_opendata_44046(share_it_monography):
    """XSLT validation with common monography (Aa)"""

    # act
    outcome = ddb_xslt_validation(share_it_monography, ignore_rules=[])

    # assert
    assert len(outcome) == 1
    assert outcome['info'] == [('identifier_01', '265982944')]


@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
def test_ddb_schematron_opendata_44046_identifier(sch_mock, share_it_monography):
    """Suppress identifier validation message for 
    simple monography (Aa)"""

    # arrange
    tmp_path = Path(os.path.dirname(share_it_monography))
    file_name= os.path.basename(share_it_monography)
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    outcome = ddb_validation(share_it_monography, ignore_rules=['identifier_01'])

    # assert
    assert sch_mock.call_count == 1
    assert 'info' not in outcome
    assert len(outcome) == 0


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_opendata_44046_ignorances_as_string(sch_mock, share_it_monography):
    """Ensure also ignore information can be passed as string"""

    # arrange
    tmp_path = Path(os.path.dirname(share_it_monography))
    file_name= os.path.basename(share_it_monography)
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    outcome = ddb_validation(share_it_monography, ignore_rules='identifier_01')

    # assert
    assert sch_mock.call_count == 1
    assert 'info' not in outcome
    assert len(outcome) == 0


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason=f'no saxonpy bindings')
def test_ddb_xslt_opendata_44046_plain(share_it_monography):
    """Suppress identifier validation message for 
    simple monography (Aa)"""

    # act
    outcome = ddb_xslt_validation(share_it_monography, ignore_rules=['identifier_01'])

    # assert
    assert 'info' not in outcome
    assert len(outcome) == 0


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason=f'no saxonpy bindings')
def test_ddb_xslt_validation_kitodo2_legacy_monography(tmp_path):
    """XSLT validation outcome for out-dated
    print export data from Kitodo2 legacy system"""

    # arrange
    file_name = 'k2_mets_vd18_147638674.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_xslt_validation(mets_target)

    # assert 
    assert len(result) == 2
    assert len(result['error']) == 2
    assert result['error'][0][0] == 'location_02'
    assert result['error'][1][0] == 'dmdSec_04'
    assert len(result['warn']) == 1
    assert result['warn'][0][0] == 'amdSec_13'


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
def test_ddb_schematron_validation_kitodo2_legacy_monography(tmp_path):
    """Schematron validation outcome for out-dated
    print export data from Kitodo2 legacy system"""

    # arrange
    file_name = 'k2_mets_vd18_147638674.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_validation(mets_target)

    # assert 
    assert len(result) == 2
    assert len(result['error']) == 2
    assert result['error'][0][0] == 'location_02'
    assert result['error'][1][0] == 'dmdSec_04'
    assert len(result['warn']) == 1
    assert result['warn'][0][0] == 'amdSec_13'  # should be PD-licence


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
def test_ddb_schematron_validation_kitodo2_legacy_monography_curated(tmp_path):
    """DDB compliant data with curated METS which doesn't 
    contain former additional dmdSec anymore so only
    a single warning regarding licences remains"""

    # arrange
    file_name = 'k2_mets_vd18_147638674_curated.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_validation(mets_target)

    # assert 
    assert len(result) == 1
    assert 'error' not in result
    assert len(result['warn']) == 1 
    assert result['warn'][0][0] == 'amdSec_13'


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason=f'no saxonpy bindings')
def test_ddb_xslt_validation_kitodo2_menadoc_44080924x(tmp_path):
    """XSLT validation outcome for rather
    recent digitized object from menadoc retro-digi"""

    # arrange
    file_name = 'k2_mets_mena_44080924X.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_xslt_validation(mets_target)

    # assert 
    assert len(result) == 2
    assert len(result['error']) == 3    # 3 errors
    assert result['error'][0][0] == 'originInfo_06'
    assert result['error'][1][0] == 'location_02'
    assert result['error'][2][0] == 'dmdSec_04'
    assert len(result['warn']) == 3     # 3 warnings
    assert result['warn'][0][0] == 'titleInfo_08'


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
def test_ddb_schematron_validation_kitodo2_menadoc_44080924x(tmp_path):
    """Schematron validation outcome for rather
    recent digitized object from menadoc retro-digi"""

    # arrange
    file_name = 'k2_mets_mena_44080924X.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_validation(mets_target)

    # assert 
    assert len(result) == 2
    assert len(result['error']) == 3    # 3 errors
    assert result['error'][0][0] == 'originInfo_06'
    assert result['error'][1][0] == 'location_02'
    assert result['error'][2][0] == 'dmdSec_04'
    assert len(result['warn']) == 3     # 3 warnings
    assert result['warn'][0][0] == 'titleInfo_08'


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason=f'no saxonpy bindings')
def test_ddb_xslt_validation_kitodo2_vd18_153142537_raw(tmp_path):
    """XSLT validation outcome for VD18 c-stage
    without default customm ignore rules"""

    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_xslt_validation(mets_target, ignore_rules=[])

    # assert 
    assert len(result) == 2
    assert len(result['error']) == 7    # 2 errors
    assert result['error'][0][0] == 'structMapLogical_17'
    assert result['error'][1][0] == 'fileSec_02'
    assert result['error'][2][0] == 'titleInfo_02'
    assert result['error'][3][0] == 'originInfo_06'
    assert result['error'][4][0] == 'originInfo_06'
    assert result['error'][5][0] == 'location_02'
    assert result['error'][6][0] == 'dmdSec_04'
    assert 'warn' not in result         # no warning
    assert len(result['info']) == 1     # 1 info
    assert result['info'] == [('identifier_01', 'GBV:153142537')]


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_validation_kitodo2_vd18_153142537_raw(sch_mock, tmp_path):
    """Schematron validation outcome for VD18 F-Stage
    without respect to custom ignore rules"""

    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    result = ddb_validation(mets_target, ignore_rules=[])

    # assert 
    assert sch_mock.call_count == 1
    assert len(result) == 2
    assert len(result['error']) == 7    # 6 errors
    assert result['error'][0][0] == 'structMapLogical_17'
    assert result['error'][1][0] == 'fileSec_02'
    assert result['error'][2][0] == 'titleInfo_02'
    assert result['error'][3][0] == 'originInfo_06'
    assert result['error'][4][0] == 'originInfo_06'
    assert result['error'][5][0] == 'location_02'
    assert result['error'][6][0] == 'dmdSec_04'
    assert 'warn' not in result         # no warning
    assert len(result['info']) == 1     # 1 info
    assert result['info'] == [('identifier_01', 'type=eki')]


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_validation_kitodo2_vd18_153142537_raw_fatals(sch_mock, tmp_path):
    """Schematron validation outcome for VD18 F-Stage
    without default ignore rules and without merging
    invalid category 'fatal' into 'error'"""

    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    result = ddb_validation(mets_target, aggregate_errors=False, ignore_rules=[])

    # assert 
    assert sch_mock.call_count == 1
    assert len(result) == 3
    assert len(result['error']) == 5    # 4 errors remain
    assert result['error'][0][0] == 'titleInfo_02'
    assert result['error'][1][0] == 'originInfo_06'
    assert result['error'][2][0] == 'originInfo_06'
    assert result['error'][3][0] == 'location_02'
    assert result['error'][4][0] == 'dmdSec_04'
    assert len(result['fatal']) == 2    # 2 fatals
    assert result['fatal'][0][0] == 'structMapLogical_17'
    assert result['fatal'][1][0] == 'fileSec_02'
    assert 'warn' not in result         # no warning
    assert len(result['info']) == 1     # 1 info
    assert result['info'][0][0] == 'identifier_01'


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_validation_kitodo2_vd18_153142537(sch_mock, tmp_path):
    """Schematron validation outcome for VD18 F-Stage
    which considers our default ignore rules"""

    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    result = ddb_validation(mets_target)

    # assert 
    assert sch_mock.call_count == 1
    assert len(result) == 1
    assert len(result['error']) == 4    # 4 errors remain
    assert result['error'][0][0] == 'originInfo_06'
    assert result['error'][1][0] == 'originInfo_06'
    assert result['error'][2][0] == 'location_02'
    assert result['error'][3][0] == 'dmdSec_04'
    assert 'warn' not in result         # no warning
    assert 'info' not in result         # no info


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
def test_ddb_schematron_validation_kitodo2_vd18_153142537_curated(tmp_path):
    """DDB compliant with curated METS which doesn't contain
    the former additional dmdSec anymore and also ignores
    originInfo_06 issues"""

    # arrange
    file_name = 'k2_mets_vd18_153142537_curated.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))
    _ignore_them = DDB_IGNORE_RULES_MVW + ['originInfo_06']

    # act
    result = ddb_validation(mets_target, ignore_rules=_ignore_them)

    # assert 
    assert len(result) == 0
    assert 'error' not in result    # no errors
    assert 'warn' not in result     # no warning
    assert 'info' not in result     # no info


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_validation_kitodo2_vd18_153142340_raw(sch_mock, tmp_path):
    """Schematron validation outcome for VD18 C-Stage
    corresponding to k2_mets_153142537.xml as-it-is"""

    # arrange
    file_name = 'k2_mets_vd18_153142340.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    result = ddb_validation(mets_target, ignore_rules=[])

    # assert 
    assert sch_mock.call_count == 1
    assert len(result) == 3
    assert len(result['error']) == 3    # 3 errors
    assert result['error'][0][0] == 'structMapLogical_17'
    assert result['error'][1][0] == 'originInfo_06'
    assert result['error'][2][0] == 'originInfo_06'
    assert len(result['warn']) == 1     # 1 warning
    assert result['warn'][0][0] == 'location_03'    # missing physicalLocation => but it's c-stage?
    assert len(result['info']) == 1     # 1 info
    assert result['info'] == [('identifier_01', 'type=eki')]


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
@mock.patch('digiflow.digiflow_validate._forward_sch_cli')
def test_ddb_schematron_validation_kitodo2_vd18_153142340(sch_mock, tmp_path):
    """Schematron validation outcome for VD18 C-Stage
    corresponding to k2_mets_153142537.xml
    considering ULB default ignore rules"""

    # arrange
    file_name = 'k2_mets_vd18_153142340.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))
    sch_mock.return_value = _place_file(tmp_path, file_name)

    # act
    result = ddb_validation(mets_target)

    # assert 
    assert sch_mock.call_count == 1
    assert len(result) == 2
    assert len(result['error']) == 2    # 2 errors remain
    assert result['error'][0][0] == 'originInfo_06'
    assert result['error'][1][0] == 'originInfo_06'
    assert len(result['warn']) == 1     # 1 warning
    assert result['warn'][0][0] == 'location_03'
    assert 'info' not in result     # no info


@pytest.mark.skipif(not os.path.isfile(SCHEMATRON_BIN), 
    reason=f'no schematron binary at {SCHEMATRON_BIN}')
def test_ddb_schematron_validation_kitodo2_morbio_1748529021(tmp_path):
    """Schematron validation outcome for recent Morbio
    export considering ULB default ignore rules"""

    # arrange
    file_name = 'k2_mets_morbio_1748529021.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result = ddb_validation(mets_target)

    # assert 
    assert len(result) == 2
    assert len(result['error']) == 3    # 3 errors
    assert result['error'][0][0] == 'location_02'
    assert result['error'][1][0] == 'dmdSec_04' # the suspicious additional dmdSec
    assert result['error'][2][0] == 'amdSec_05' # contains cc-by-3 => not accepted
    assert 'warn' not in result     # no warnings
    assert len(result['info']) == 2 # 2 info
    assert result['info'][0][0] == 'subject_01' # subject/topic without gnd-link "Sonstige..."
    assert result['info'][1][0] == 'subject_01' # subject/topic without gnd-link "Notarsurkunde"
