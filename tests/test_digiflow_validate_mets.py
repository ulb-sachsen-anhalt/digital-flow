# -*- coding: utf-8 -*-
"""Digiflow DDB conformant validation API"""

import os
import shutil

from pathlib import (
    Path
)

import pytest

from digiflow.validate.metadata_ddb import (
    IGNORE_DDB_RULES_ULB,
    DDBRole,
    Report,
    Reporter,
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


def test_role_order():
    """Ensure roles order matters"""

    assert DDBRole.FATAL > DDBRole.ERROR
    assert DDBRole.INFO < DDBRole.FATAL
    assert DDBRole.FATAL >= DDBRole.FATAL


@pytest.mark.parametrize(
    "input_string,expected_obj", [
        ('caution', DDBRole.CAUTION),
        ('error', DDBRole.ERROR),
        ('fatal', DDBRole.FATAL),
        ('foo', None),
        (None, None),
    ])
def test_role_for_label(input_string, expected_obj):
    """Ensure enum objects found for given input"""

    assert DDBRole.from_label(input_string) is expected_obj


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_report_newspaper_headlines(tmp_path):
    """Simple MWE to trigger transformation including
    schematron failures => none in this case, all fine
    """

    # arrange
    file_name = 'vls_digitale_9633116.zmets.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = os.path.join(tmp_path, file_name)
    shutil.copy(str(mets_source), str(mets_target))
    reporter = Reporter(mets_target, digi_type='OZ')

    # act
    whats_up: Report = reporter.get()

    # assert
    assert len(whats_up.ddb_meldungen) == 2
    assert ('error', 'amdSec_04') == whats_up.read()[0]
    assert ('warn', 'structMapLogical_12') == whats_up.read()[1]


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_report_newspaper_longform(tmp_path):
    """Simple MWE to trigger transformation including
    schematron failures => none in this case, all fine
    """

    # arrange
    file_name = 'vls_digitale_9633116.zmets.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = os.path.join(tmp_path, file_name)
    shutil.copy(str(mets_source), str(mets_target))
    reporter = Reporter(mets_target, digi_type='OZ')

    # act
    whats_up: Report = reporter.get()

    # assert
    assert len(whats_up.ddb_meldungen) == 2
    assert "error(1x):['amdSec_04']" == whats_up.read(map_ddb_roles=True)[0]
    assert "warn(1x):['structMapLogical_12']" == whats_up.read(map_ddb_roles=True)[1]


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


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_opendata_44046_defaults(share_it_monography):
    """Schematron validation with common monography (Aa)
    and default ignorance yield no problems and no
    default alert

    BUT

    if role min level adopted, then yield alert
    """

    # act
    reporter = Reporter(share_it_monography)
    whats_up: Report = reporter.get(min_ddb_level='info')

    # assert
    assert not whats_up.alert()         # nothing very bad per se
    assert len(whats_up.ddb_meldungen) == 1  # but still one meldung
    assert ('info', 'identifier_01') == whats_up.read()[0]
    assert whats_up.alert('info')   # provoke alert
    assert reporter.input_conform()


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_opendata_44046_defaults_not_conform(share_it_monography):
    """XSD validation yields null problemo"""

    # act
    reporter = Reporter(share_it_monography)
    _ = reporter.get()

    # assert
    assert reporter.input_conform()
    assert not reporter.get().alert()


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_opendata_44046_ignore_ident01(share_it_monography):
    """Suppress identifier validation message for 
    simple monography (Aa)"""

    # act
    whats_up: Report = Reporter(share_it_monography).get(ignore_ddb_rule_ids=['identifier_01'])
    assert not whats_up.alert()
    assert len(whats_up.ddb_meldungen) == 0


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_legacy_monography_raw(tmp_path):
    """XSLT validation outcome for out-dated
    print export data from Kitodo2 legacy system

    Please note, that any other roles like info or warning
    (=> amdSec_13, amdSec_15) will be swallowed

    changed: 4.3+
    no more exception thrown, but .alert() yields TRUE
    """

    # arrange
    file_name = 'k2_mets_vd18_147638674.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    reporter = Reporter(mets_source)
    whats_up: Report = reporter.get()

    # assert
    loads = whats_up.read()
    assert whats_up.alert()
    assert len(whats_up.ddb_meldungen) == 6
    assert ('fatal', 'fileSec_02') == loads[0]
    assert ('error', 'titleInfo_02') == loads[1]
    assert ('error', 'location_01') == loads[2]
    assert ('error', 'dmdSec_04') == loads[3]
    assert ('caution', 'amdSec_13') == loads[4]
    assert ('warn', 'amdSec_15') == loads[5]
    assert reporter.input_conform()


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_legacy_monography_curated(tmp_path):
    """DDB compliant data with curated METS which doesn't 
    contain former additional dmdSec anymore so only
    warnings concerning licence and digiprovMD remain

    FATAL for fileSec_02 would remain otherwise,
    since this contains no DEFAULT image section
    """

    # arrange
    file_name = 'k2_mets_vd18_147638674_curated.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result: Report = Reporter(mets_source).get(ignore_ddb_rule_ids=['fileSec_02'])

    # assert
    assert not result.alert()
    assert len(result.read()) == 2
    assert ('caution', 'amdSec_13') == result.read()[0]


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
    result: Report = Reporter(mets_source).get(min_ddb_level='info')

    # assert
    loads = result.read()
    assert len(loads) == 26
    loads_mapped = result.read(map_ddb_roles=True)
    assert len(loads_mapped) == 5   # all five rule roles
    assert result.alert()
    assert "fatal(1x):['fileSec_02']" == loads_mapped[0]


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142537_raw(tmp_path):
    """XSLT validation outcome for VD18 c-stage
    without default custom ignore rules

    changed from 05/22 => 06/23
    * increased errors from 7 to 11
    changed from 06/23 => 04/24
    * increased meldungen from 11 to 15
    """

    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result: Report = Reporter(mets_source).get(min_ddb_level='info')

    # assert
    loads = result.read()
    assert len(loads) == 15
    assert result.alert()


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142537(tmp_path):
    """Validation outcome for VD18 F-Stage
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
    reporter = Reporter(mets_source)
    result: Report = reporter.get(ignore_ddb_rule_ids=IGNORE_DDB_RULES_ULB,
                                  min_ddb_level='info')

    # assert
    loads = result.read()
    assert len(loads) == 4
    assert not result.alert()
    assert loads[0] == ('error', 'location_01')
    assert loads[1] == ('error', 'dmdSec_04')
    assert loads[2] == ('warn', 'amdSec_15')
    assert loads[3] == ('info', 'identifier_01')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142537_curated(tmp_path):
    """DDB compliant with curated METS which doesn't contain
    former additional dmdSec anymore and also ignores
    all DDB rules below 'error'
    """

    # arrange
    # arrange
    file_name = 'k2_mets_vd18_153142537.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    ignore_them = IGNORE_DDB_RULES_ULB
    result: Report = Reporter(mets_source).get(ignore_them, 'error')

    # assert
    loads = result.read()
    assert len(loads) == 2
    assert loads[0] == ('error', 'location_01')
    assert loads[1] == ('error', 'dmdSec_04')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142340_raw(tmp_path):
    """Schematron validation outcome for VD18 C-Stage
    corresponding to k2_mets_153142537.xml as-it-is

    changed 2024/08
    * due min role level set to warning now having 5 meldungen
      (instead of 3 before)
    """

    # arrange
    file_name = 'k2_mets_vd18_153142340.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result: Report = Reporter(mets_source).get()

    # assert
    loads = result.read()
    assert result.alert()
    assert len(loads) == 5
    assert loads[0] == ('fatal', 'structMapLogical_17')
    assert loads[1] == ('error', 'originInfo_06')
    assert loads[2] == ('error', 'originInfo_06')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_ddb_validate_kitodo2_vd18_153142340(tmp_path):
    """Schematron validation outcome for VD18 C-Stage
    corresponding to k2_mets_153142537.xml
    considering ULB default ignore rules"""

    # arrange
    file_name = 'k2_mets_vd18_153142340.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result: Report = Reporter(mets_source).get(min_ddb_level='error')

    # assert
    loads = result.read()
    assert len(loads) == 3
    assert result.alert()
    assert loads[0] == ('fatal', 'structMapLogical_17')
    assert loads[1] == ('error', 'originInfo_06')
    assert loads[2] == ('error', 'originInfo_06')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_report_about_kitodo2_morbio_1748529021_ddb(tmp_path):
    """Schematron validation outcome for recent Morbio
    export considering ULB default ignore rules

    changed due 2023/06: dropped failure for amdSec_05 (licence)

    changed due 2024/08
    * some more DDB meldungen poping up
    * combined with XSD schema validation yields

    """

    # arrange
    file_name = 'k2_mets_morbio_1748529021.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result: Report = Reporter(mets_source).get()

    # assert
    loads = result.read()
    assert result.alert()
    assert len(loads) == 6    # 4 meldungen total
    assert loads[0] == ('fatal', 'fileSec_02')
    assert loads[1] == ('error', 'titleInfo_02')
    assert loads[2] == ('error', 'location_01')
    assert loads[3] == ('error', 'dmdSec_04')   # suspicious additional dmdSec


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_report_about_kitodo2_morbio_1748529021_xsd(tmp_path):
    """Due combination with XSD schema validation
    we catch now schema error concerning accessCondition
    """

    # arrange
    file_name = 'k2_mets_morbio_1748529021.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result: Report = Reporter(mets_source).get()

    # assert
    assert result.alert()
    assert result.xsd_errors[0][0] == 'ERROR'
    assert result.xsd_errors[0][1] == 'SCHEMASV'
    assert result.xsd_errors[0][2] == "Element '{http://www.loc.gov/mods/v3}accessCondition', attribute 'href': The attribute 'href' is not allowed."


def test_ddb_validate_newspaper(tmp_path):
    """Test recent DDB-Validation 06/23 for
    migrated newspaper issue missing
    * license information

    changed 2024/08
    * one more warning poping up considering missing FULLTEXT
    """

    # arrange
    the_name = 'vls_digital_3014754.zmets.xml'
    mets_source = Path(TEST_RES) / the_name
    mets_target = Path(str(tmp_path), the_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    result: Report = Reporter(mets_source, digi_type='OZ').get(min_ddb_level='warn')

    # assert
    loads = result.read()
    assert not result.alert()
    assert len(loads) == 2
    assert loads[0] == ('error', 'amdSec_04')


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
    result: Report = Reporter(mets_source, digi_type='OZ').get()

    # assert
    assert len(result.ddb_meldungen) == 0


def test_ddb_validate_opendata_origin_info_mystery():
    """Why error although set to ignore?
    Recent behavior was to override ignore rules 
    if digitalization type for 'medien' recognize

    changed 2024/08
    * popping up structMapLogical_27 warning concerning struct types
      1x for 'cover_front'
      1x for 'cover_back'
    """

    # arrange
    the_mets = '1981185920_34752.xml'
    mets_path = TEST_RES / 'opendata' / the_mets
    ignore_these = ['identifier_01', 'originInfo_06']

    # act
    reporter = Reporter(mets_path, digi_type='AF')
    report: Report = reporter.get(ignore_ddb_rule_ids=ignore_these)

    # assert
    assert not report.alert()
    assert len(report.ddb_meldungen) == 2
    assert report.read()[0] == ('warn', 'structMapLogical_27')
    assert report.read()[1] == ('warn', 'structMapLogical_27')
