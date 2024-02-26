# -*- coding: utf-8 -*-
"""Digiflow validation API"""

import os
import shutil

from pathlib import (
    Path
)

import lxml.etree as ET

import pytest

from digiflow.digiflow_validate import (
    DigiflowDDBException,
    apply,
    gather_failed_asserts,
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


TEST_XSL = str(TEST_RES / 'xsl' / 'zeitungen_ulb.xsl')


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_apply_xslt_issue_with_gathering(tmp_path):
    """Simple MWE to trigger transformation including
    schematron failures => none in this case, all fine
    """

    # arrange
    file_name = 'vls_digitale_9633116.zmets.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = os.path.join(tmp_path, file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    _result = apply(mets_target, path_xslt=TEST_XSL, 
                    post_process=gather_failed_asserts)

    # assert
    assert _result == {}


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_apply_xslt_issue_plain(tmp_path):
    """Trigger XSLT without any postprocessing =>
    result file written, check this out
    """

    # arrange
    file_name = 'vls_digitale_9633116.zmets.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = os.path.join(tmp_path, file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    _result_file: Path = apply(mets_target, path_xslt=TEST_XSL)

    # assert
    assert _result_file.exists()
    _the_root = ET.parse(_result_file).getroot()
    _map = _the_root.nsmap
    _fired_rules = _the_root.xpath('//svrl:fired-rule/text()', namespaces=_map)
    assert len(_fired_rules) == 1
    
    assert _fired_rules[0] == "Datumsangaben passen: 1840-12-31 = 1840-12-31"


@pytest.fixture(name='corrupted_issue')
def _fixture_corrupted_issue(tmp_path):
    """Create issue with mismatching information
    between publication date and day@ORDERLABEL
    
    replacing
    <mods:dateIssued encoding="iso8601">1840-12-31</mods:dateIssued>
    with
    <mods:dateIssued encoding="iso8601">1840-12-30</mods:dateIssued>

    (Therefore publication origin and @ORDERLABEL no longer fit.)
    """

    file_name = 'vls_digitale_9633116.zmets.xml'
    _source = Path(TEST_RES) / file_name
    _target = os.path.join(tmp_path, file_name)
    shutil.copy(str(_source), str(_target))
    _needle = '<mods:dateIssued encoding="iso8601">1840-12-31</mods:dateIssued>'
    _replac = '<mods:dateIssued encoding="iso8601">1840-12-30</mods:dateIssued>'
    _lines = []
    with open(_target, 'r') as _org_file:
        _lines = _org_file.readlines()
    for i, _line in enumerate(_lines):
        if _needle in _line:
            _lines[i] = _line.replace(_needle, _replac)
    with open(_target, 'w') as _corrupted:
        _corrupted.writelines(_lines)
    yield _target


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_apply_xslt_issue_corrupted_file(corrupted_issue):
    """Corrupted issue without post_processing
    """

    # act
    _result_file: Path = apply(corrupted_issue, path_xslt=TEST_XSL)

    # assert
    assert _result_file.exists()
    _the_root = ET.parse(_result_file).getroot()
    _map = _the_root.nsmap
    _failed = _the_root.xpath('//svrl:failed-assert', namespaces=_map)[0]
    assert "date_mets_to_mods" in _failed.get('id')
    assert "fatal" in _failed.get('role')
    assert "Logisches Datum passt nicht" in _failed[0].text


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_apply_xslt_issue_corrupted_gathering_failures(corrupted_issue):
    """Simple MWE to trigger transformation including
    schematron failures => none in this case, all fine
    """

    # act
    with pytest.raises(DigiflowDDBException) as _exc:
        apply(corrupted_issue, path_xslt=TEST_XSL, 
              post_process=gather_failed_asserts)

    # assert
    _fail_str = '[date_mets_to_mods]  (Logisches Datum passt nicht zu Publikationsdatum: 1840-12-31 != 1840-12-30)'
    assert _fail_str == _exc.value.args[0][0]
