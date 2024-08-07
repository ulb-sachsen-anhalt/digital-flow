# -*- coding: utf-8 -*-
"""Digiflow API for enhanced XSLT 2.0 handling"""

import os
import shutil

from pathlib import (
    Path
)

import lxml.etree as ET

import pytest

import digiflow.validate.metadata_xslt as df_vmdx

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
    _result_file: Path = df_vmdx.transform(mets_target, path_template=TEST_XSL)

    # assert
    assert _result_file.exists()
    the_root = ET.parse(_result_file).getroot()
    ns_map = the_root.nsmap
    fired_rules = the_root.xpath('//svrl:fired-rule/text()', namespaces=ns_map)
    assert len(fired_rules) == 1
    assert fired_rules[0] == "Datumsangaben passen: 1840-12-31 = 1840-12-31"


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
    the_source = Path(TEST_RES) / file_name
    the_target = os.path.join(tmp_path, file_name)
    shutil.copy(str(the_source), str(the_target))
    a_needle = '<mods:dateIssued encoding="iso8601">1840-12-31</mods:dateIssued>'
    to_replac = '<mods:dateIssued encoding="iso8601">1840-12-30</mods:dateIssued>'
    lines = []
    with open(the_target, 'r') as _org_file:
        lines = _org_file.readlines()
    for i, the_line in enumerate(lines):
        if a_needle in the_line:
            lines[i] = the_line.replace(a_needle, to_replac)
    with open(the_target, 'w') as corrupted:
        corrupted.writelines(lines)
    yield the_target


@pytest.mark.skipif(not SAXON_PY_ENABLED, reason='no saxon binary')
def test_apply_xslt_issue_corrupted_file(corrupted_issue):
    """Corrupted issue without post_processing
    """

    # act
    result_file: Path = df_vmdx.transform(corrupted_issue, path_template=TEST_XSL)

    # assert
    assert result_file.exists()
    the_root = ET.parse(result_file).getroot()
    ns_map = the_root.nsmap
    failures = the_root.xpath('//svrl:failed-assert', namespaces=ns_map)[0]
    assert "date_mets_to_mods" in failures.get('id')
    assert "fatal" in failures.get('role')
    assert "Logisches Datum passt nicht" in failures[0].text
    assert "1840-12-31 != 1840-12-30"  in failures[0].text
