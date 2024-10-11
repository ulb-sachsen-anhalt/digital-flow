"""Specification for IO API"""

import ast
import os
import shutil
import unittest.mock
import uuid

from pathlib import Path

import pytest

import lxml.etree as ET

import digiflow.digiflow_io as df_io
import digiflow.digiflow_metadata as df_md

from .conftest import TEST_RES

ROOT = Path(__file__).parents[1]

EXPORT_METS = 'export_mets.xml'

# some test constants
ID_737429 = '737429'
OAI_ID_737429 = f'oai:digital.bibliothek.uni-halle.de/hd:{ID_737429}'
OAI_SPEC_737429 = 'ulbhaldod'
CONTENT_TYPE_TXT = 'text/xml;charset=utf-8'
OAI_BASE_URL_VD16 = 'digitale.bibliothek.uni-halle.de/vd16/oai'
OAI_BASE_URL_ZD = 'digitale.bibliothek.uni-halle.de/zd/oai'
OAI_BASE_URL_OPENDATA = 'opendata.uni-halle.de/oai/dd'

# pylint: disable=c-extension-no-member, line-too-long


@pytest.mark.skipif("sys.version_info < (3,6)")
def test_intermediate_dirs_created_with_path(tmp_path):
    """Test depends on PosixPath, only works with 3.6+"""

    src_path = TEST_RES / "k2_mets_vd18_147638674.xml"
    path_dst = tmp_path / "sub_dir" / "another_sub_dir" / "147638674.xml"
    xml = ET.parse(src_path)

    # act
    df_md.write_xml_file(xml.getroot(), path_dst)

    assert os.path.isfile(path_dst)


def test_intermediate_dirs_created_with_tmpdir(tmpdir):
    """Test depends on PosixPath, only works with 3.6+"""

    src_path = TEST_RES / "k2_mets_vd18_147638674.xml"
    path_dst = tmpdir.join("sub_dir").mkdir().join(
        "another_sub_dir").mkdir().join("147638674.xml")
    xml = ET.parse(src_path)

    # act
    df_md.write_xml_file(xml.getroot(), str(path_dst))

    assert os.path.isfile(str(path_dst))


def test_write_xml_defaults(tmp_path):
    """Output with deafult write settings"""

    txt = '<parent><child name="foo">bar</child></parent>'
    xml_tree = ET.fromstring(txt)
    outpath = tmp_path / "write_foo.xml"
    df_md.write_xml_file(xml_tree, str(outpath))

    assert os.path.isfile(str(outpath))
    assert open(str(outpath), encoding='utf8').read().startswith(
        '<?xml version="1.0" encoding="UTF-8"?>\n')


def test_write_xml_without_preamble(tmp_path):
    """Test output if no preamble required"""

    txt = '<parent><child name="foo">bar</child></parent>'
    xml_tree = ET.fromstring(txt)
    outpath = tmp_path / "write_foo.xml"
    df_md.write_xml_file(xml_tree, str(outpath), preamble=None)

    assert os.path.isfile(str(outpath))
    assert open(str(outpath), encoding='utf8').read().startswith('<parent>\n')


def fixture_request_results(*args, **kwargs):
    """
    Provide local copies for corresponding download request
    * dd/oai:opendata.uni-halle.de:1981185920/36020
    """
    the_url = args[0]
    the_headers = kwargs['headers'] if 'headers' in kwargs else {}
    result = unittest.mock.Mock()
    result.status_code = 200
    result.headers = {'Content-Type': 'image/jpeg'}
    if the_headers:
        for k, v in the_headers.items():
            result.headers[k] = v
        # , 'User-Agent': the_headers['User-Agent']}
    max_image_dir = os.path.join(
        str(ROOT), 'tests/resources/vls/monography/737429/MAX')
    # this one is the METS/MODS
    if the_url.endswith('36020'):
        result.headers['Content-Type'] = CONTENT_TYPE_TXT
        data_path = os.path.join(
            str(ROOT), 'tests/resources/opendata/1981185920_36020.oai.xml')
        with open(data_path, encoding="utf-8") as xml:
            result.content = xml.read()
    elif the_url.endswith('997508'):
        result.headers['Content-Type'] = CONTENT_TYPE_TXT
        data_path = os.path.join(
            str(ROOT), 'tests/resources/vls/vd16-oai-997508.xml')
        with open(data_path, encoding="utf-8") as xml:
            result.content = xml.read()
    else:
        with open(max_image_dir + '/737434.jpg', 'rb') as img:
            result.content = img.read()
    return result


@pytest.fixture(name="migration_sweeper_img_fixture")
def _fixture_migration_img_sweeper(tmp_path):
    """Provide test fixture"""

    testroot = tmp_path / 'SWEEPERTEST'
    testroot.mkdir()
    maxdir = testroot / 'MAX'
    file_name = '737429.mets.xml'
    testresources = Path(ROOT / 'tests' / 'resources' / 'migration')
    mets_source = testresources / file_name
    shutil.copy(str(mets_source), os.path.join(str(testroot), file_name))
    images_dir = testresources / 'MAX'
    shutil.copytree(str(images_dir), str(maxdir))
    return str(testroot)


def test_migration_sweeper_img(migration_sweeper_img_fixture):
    """Test cleanup images and renaming of mets file"""

    df_io.OAIFileSweeper(migration_sweeper_img_fixture).sweep()

    for item in Path(migration_sweeper_img_fixture).iterdir():
        if str(item.name) == 'MAX':
            # preserve colorchecker
            assert len(list(item.iterdir())) == 1


@pytest.fixture(name="migration_sweeper_pdf_fixture")
def _fixture_migration_pdf_sweeper(tmp_path):
    """Provide test fixture"""

    testroot = tmp_path / 'SWEEPERTEST'
    testroot.mkdir()
    downloaddir = testroot / 'DOWNLOAD'
    downloaddir.mkdir()
    file_name = '1981185920_44046.xml'
    pdf_file_name = '265982944.pdf'
    testresources = Path(ROOT / 'tests' / 'resources' / 'ocr')
    mets_source = testresources / file_name
    shutil.copy(str(mets_source), os.path.join(str(testroot), file_name))
    #  downloaddir.write_bytes('data')
    with open(downloaddir / pdf_file_name, 'wb') as fh:
        fh.write(b'arbitrary pdf')
    return str(testroot)


def test_migration_sweeper_pdf(migration_sweeper_pdf_fixture):
    """Test cleanup images and renaming of mets file"""

    oais = df_io.OAIFileSweeper(
        migration_sweeper_pdf_fixture, pattern=".xml", filegroups=['DOWNLOAD'])
    oais.sweep()

    for item in Path(migration_sweeper_pdf_fixture).iterdir():
        if str(item.name) == 'DOWNLOAD':
            assert len(list(item.iterdir())) == 0


@unittest.mock.patch('digiflow.digiflow_io.smtplib.SMTP')
def test_send_mail(mock_smtp):
    """Test note using SMTP mock because
    tails fails outside proper plattform
    without reachable local smto-host

    Mock called 3 times
    * object init
    * object sends message
    * object quits connection
    """

    # arrange
    random_message = uuid.uuid4().hex

    # act
    mess = df_io.smtp_note(
        'localhost:25',
        subject='test',
        message=random_message,
        froms='test@example.com',
        tos='me@example.de')

    # assert
    assert random_message in mess
    assert 'me@example.de' in mess
    assert mock_smtp.called
    assert len(mock_smtp.mock_calls) == 3
    assert mock_smtp.mock_calls[1][0] == '().send_message'


def mock_response(**kwargs):
    """Create custum mock object"""

    the_response = unittest.mock.MagicMock()
    the_response.reason = 'testing reason'
    if 'reason' in kwargs:
        the_response.reason = kwargs['reason']
    if 'status_code' in kwargs:
        the_response.status_code = int(kwargs['status_code'])
    if 'headers' in kwargs:
        the_response.headers = kwargs['headers']
    if 'data_path' in kwargs:
        with open(kwargs['data_path'], encoding="utf-8") as xml:
            the_response.content = xml.read().encode()
    return the_response


@unittest.mock.patch('requests.get')
def test_response_404(mock_requests: unittest.mock.Mock):
    """test request ends up with 417"""

    # arrange
    the_response = mock_response(status_code=417)
    mock_requests.return_value = the_response

    # act
    with pytest.raises(df_io.LoadException) as exc:
        df_io.request_resource('http://foo.bar', Path())

    # assert
    assert exc.typename == 'ClientError'
    assert "http://foo.bar status 417" == exc.value.args[0]


@unittest.mock.patch('requests.get')
def test_response_200_with_error_content(mock_requests: unittest.mock.Mock):
    """test request results into OAILoadException"""

    # arrange
    data_path = os.path.join(str(ROOT), 'tests/resources/opendata/id_not_exist.xml')
    a_response = mock_response(status_code=200,
                               headers={'Content-Type': 'text/xml;charset=UTF-8'},
                               data_path=data_path)
    mock_requests.return_value = a_response

    # act
    with pytest.raises(df_io.LoadException) as exc:
        df_io.request_resource('http://foo.bar', Path())

    assert 'verb requires' in str(exc.value)


def test_call_requests_kwargs_invalid_str(tmp_path):
    """Explore behavior when trying to pass kwargs
    Please note: pylint would warn also if active
    """

    # arrange
    the_url = "https://dumy.com"
    the_kwargs = 'timeout=20, headers={"User-Agent": "ulbbot+IT-WF-OCR-VD17"}'

    # act
    with pytest.raises(TypeError) as terr:
        # pylint:disable=not-a-mapping
        df_io.request_resource(the_url, path_local=tmp_path, **the_kwargs)

    # assert
    assert "argument after ** must be a mapping" in terr.value.args[0]


@unittest.mock.patch("requests.get")
def test_call_requests_kwargs_empty(mock_requests: unittest.mock.Mock, tmp_path):
    """Behavior when passinf empty dict
    Still raises error due mocked request response
    (which is not important this time)
    """

    # arrange
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.status_code = 200
    the_url = "https://dumy.com"
    the_kwargs = {}

    # act
    with pytest.raises(df_io.ContentException) as strange:
        df_io.request_resource(the_url, path_local=tmp_path, **the_kwargs)

    # assert
    assert "unhandled content-type" in strange.value.args[0]
    assert mock_requests.call_count == 1


@unittest.mock.patch("requests.get")
def test_call_requests_kwargs_valid(mock_requests: unittest.mock.Mock, tmp_path):
    """Behavior when trying to pass valid kwargs
    Still raises error due mocked request response
    (which is not important this time)
    """

    # arrange
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.status_code = 200
    the_url = "https://dumy.com"
    raw_kwargs = 'timeout=20, headers={"User-Agent": "ulbbot+IT-WF-OCR-VD17"}'
    top_tokens = raw_kwargs.split(",")
    the_kwargs = {}
    for t in top_tokens:
        k, v = t.split("=", maxsplit=1)
        the_kwargs[k] = ast.literal_eval(v)

    # act
    with pytest.raises(df_io.ContentException) as strange:
        df_io.request_resource(the_url, path_local=tmp_path, **the_kwargs)

    # assert
    assert "unhandled content-type" in strange.value.args[0]
    assert mock_requests.call_count == 1


@unittest.mock.patch("requests.get")
def test_oailoader_with_string_requests_kwargs(mock_requests: unittest.mock.Mock, tmp_path):
    """Behavior when using the regular OAILoader with
    string kwargs from a configuration
    Still raises error due mocked request response
    (which is not important this time)
    """

    # arrange
    mock_requests.return_value.status_code = 200
    mock_requests.return_value.status_code = 200
    the_url = "https://dumy.com"
    raw_kwargs = {
        df_io.OAI_KWARG_REQUESTS: 'timeout=20, headers={"User-Agent": "ulbbot+IT-WF-OCR-VD17"}'}
    loader = df_io.OAILoader(tmp_path, the_url, **raw_kwargs)

    # act
    with pytest.raises(df_io.ContentException) as strange:
        loader.load_resource(the_url, tmp_path, None)

    # assert
    assert "unhandled content-type" in strange.value.args[0]
    assert mock_requests.call_count == 1
