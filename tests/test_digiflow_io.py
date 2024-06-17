"""Specification for IO API"""

import os
import shutil
import unittest.mock
import uuid

from pathlib import Path

import pytest

import lxml.etree as ET

from digiflow import (
    OAIFileSweeper,
    LoadException,
    OAILoader,
    LocalStore,
    extract_mets,
    request_resource,
    smtp_note,
    write_xml_file,
)

import digiflow.record as df_r

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
    write_xml_file(xml.getroot(), path_dst)

    assert os.path.isfile(path_dst)


def test_intermediate_dirs_created_with_tmpdir(tmpdir):
    """Test depends on PosixPath, only works with 3.6+"""

    src_path = TEST_RES / "k2_mets_vd18_147638674.xml"
    path_dst = tmpdir.join("sub_dir").mkdir().join(
        "another_sub_dir").mkdir().join("147638674.xml")
    xml = ET.parse(src_path)

    # act
    write_xml_file(xml.getroot(), str(path_dst))

    assert os.path.isfile(str(path_dst))


def test_write_xml_defaults(tmp_path):
    """Output with deafult write settings"""

    txt = '<parent><child name="foo">bar</child></parent>'
    xml_tree = ET.fromstring(txt)
    outpath = tmp_path / "write_foo.xml"
    write_xml_file(xml_tree, str(outpath))

    assert os.path.isfile(str(outpath))
    assert open(str(outpath), encoding='utf8').read().startswith(
        '<?xml version="1.0" encoding="UTF-8"?>\n')


def test_write_xml_without_preamble(tmp_path):
    """Test output if no preamble required"""

    txt = '<parent><child name="foo">bar</child></parent>'
    xml_tree = ET.fromstring(txt)
    outpath = tmp_path / "write_foo.xml"
    write_xml_file(xml_tree, str(outpath), preamble=None)

    assert os.path.isfile(str(outpath))
    assert open(str(outpath), encoding='utf8').read().startswith('<parent>\n')


@pytest.mark.parametrize(['urn', 'local_identifier'],
                         [
    ('oai:digital.bibliothek.uni-halle.de/hd:10595', '10595'),
    ('oai:digitale.bibliothek.uni-halle.de/vd18:9427342', '9427342'),
    ('oai:opendata.uni-halle.de:1981185920/34265', '1981185920_34265'),
    ('oai:dev.opendata.uni-halle.de:123456789/27949', '123456789_27949'),
])
def test_record_local_identifiers(urn, local_identifier):
    """Ensure local identifier for different URN inputs"""

    # act
    record = df_r.Record(urn)
    assert record.local_identifier == local_identifier


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


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_vd16_with_localstore(mock_request_vd16_997508, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_vd16_997508.side_effect = fixture_request_results
    ident = 'oai:digitale.bibliothek.uni-halle.de/vd16:997508'
    record = df_r.Record(ident)
    _id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / _id
    store_dir = tmp_path / "STORE" / "dd" / _id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + _id + '.xml'

    # act
    loader = OAILoader(local_dir, base_url='digitale.bibliothek.uni-halle.de/vd16/oai',
                       group_images=key_images,
                       post_oai=extract_mets)
    loader.store = LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst, 'md997508')

    # assert first download of 1 xml + 12 image resources
    assert number == 13
    assert mock_request_vd16_997508.call_count == 13
    assert os.path.isfile(str(local_dir / (_id + ".xml")))
    assert os.path.isfile(str(local_dir / "MAX" / "1019932.jpg"))

    # ensure no subsequent re-load took place
    assert not loader.load(record.identifier, local_dst, 'md997508')
    assert mock_request_vd16_997508.call_count == 13


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_opendata_with_localstore(
        mock_request_1981185920_36020, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_1981185920_36020.side_effect = fixture_request_results
    ident = 'oai:opendata.uni-halle.de:1981185920/36020'
    record = df_r.Record(ident)
    _id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / _id
    store_dir = tmp_path / "STORE" / "dd" / _id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + _id + '.xml'

    # act
    loader = OAILoader(local_dir, base_url=OAI_BASE_URL_OPENDATA,
                       group_images=key_images,
                       post_oai=extract_mets)
    loader.store = LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst)

    # assert
    assert number == 12
    assert os.path.isdir(str(local_dir))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isfile(str(local_dir / (_id + ".xml")))
    assert os.path.isfile(str(local_dir / "MAX" / "00000011.jpg"))

    # check cache
    assert os.path.exists(str(store_dir))

    # check no re-load took place
    assert not loader.load(record.identifier, local_dst)


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_opendata_request_kwargs(
        mock_request_1981185920_36020, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_1981185920_36020.side_effect = fixture_request_results
    ident = 'oai:opendata.uni-halle.de:1981185920/36020'
    record = df_r.Record(ident)
    _id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / _id
    store_dir = tmp_path / "STORE" / "dd" / _id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + _id + '.xml'
    request_kwargs = dict(headers={'User-Agent': 'Smith'})

    # act
    loader = OAILoader(local_dir, base_url=OAI_BASE_URL_OPENDATA,
                       group_images=key_images,
                       post_oai=extract_mets,
                       request_kwargs=request_kwargs)
    loader.store = LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst)

    # assert
    assert number == 12
    assert os.path.isdir(str(local_dir))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isfile(str(local_dir / (_id + ".xml")))
    assert os.path.isfile(str(local_dir / "MAX" / "00000011.jpg"))

    # check cache
    assert os.path.exists(str(store_dir))


def fixture_request_vls_zd1_16359609(*args, **kwargs):
    """
    Provide local copies for corresponding download request
    Data: oai:digitale.bibliothek.uni-halle.de/zd:16359609
    """
    the_url = args[0]
    the_headers = kwargs['headers'] if 'headers' in kwargs else {}
    result = unittest.mock.Mock()
    result.status_code = 200
    result.headers = {'Content-Type': CONTENT_TYPE_TXT}
    if the_headers:
        for k, v in the_headers.items():
            result.headers[k] = v
        # , 'User-Agent': the_headers['User-Agent']}
    max_image_dir = os.path.join(
        str(ROOT), 'tests/resources/vls/monography/737429/MAX')
    # this one is the METS/MODS
    if the_url.endswith('16359609'):
        data_path = os.path.join(
            str(ROOT), 'tests/resources/vls/zd/zd1-16359609.mets.xml')
        with open(data_path, encoding="utf-8") as xml:
            result.content = xml.read()
    elif 'download/webcache/' in the_url:
        result.headers = {'Content-Type': 'image/jpeg'}
        with open(max_image_dir + '/737434.jpg', 'rb') as img:
            result.content = img.read()
    elif 'download/fulltext/' in the_url:
        alto_file = os.path.join(str(ROOT), 'tests/resources/vls/zd/')
        with open(alto_file + 'zd1-alto-16331001.xml', encoding="utf-8") as hndl:
            result.content = hndl.read().encode()
    return result


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_vls_zd1_with_ocr(mock_request, tmp_path):
    """Behavior of state lists for ocr pipeline
    """

    # arrange
    mock_request.side_effect = fixture_request_vls_zd1_16359609
    ident = 'oai:digitale.bibliothek.uni-halle.de/zd:16359609'
    record = df_r.Record(ident)
    _id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / _id
    store_dir = tmp_path / "STORE" / "zd" / _id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    local_dst = str(local_dir) + '/' + _id + '.xml'

    # act
    loader = OAILoader(local_dir, base_url=OAI_BASE_URL_ZD,
                       post_oai=extract_mets)
    loader.store = LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst)

    # assert
    assert number == 17
    assert os.path.isdir(str(local_dir))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isfile(str(local_dir / (_id + ".xml")))
    assert os.path.isfile(str(local_dir / "MAX" / "16331052.jpg"))

    # check cache
    assert os.path.exists(str(store_dir))


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

    OAIFileSweeper(migration_sweeper_img_fixture).sweep()

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

    oais = OAIFileSweeper(
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
    mess = smtp_note(
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

    _response = unittest.mock.MagicMock()
    _response.reason = 'testing reason'
    if 'reason' in kwargs:
        _response.reason = kwargs['reason']
    if 'status_code' in kwargs:
        _response.status_code = int(kwargs['status_code'])
    if 'headers' in kwargs:
        _response.headers = kwargs['headers']
    if 'data_path' in kwargs:
        with open(kwargs['data_path'], encoding="utf-8") as xml:
            _response.content = xml.read().encode()
    return _response


@unittest.mock.patch('requests.get')
def test_response_404(mock_requests):
    """test request ends up with 417"""

    # arrange
    _req = mock_response(status_code=417)
    mock_requests.return_value = _req

    # act
    with pytest.raises(LoadException) as exc:
        request_resource('http://foo.bar', Path())

    # assert
    assert exc.typename == 'ClientError'
    assert "url 'http://foo.bar' returned '417'" == exc.value.args[0]


@unittest.mock.patch('requests.get')
def test_response_200_with_error_content(mock_requests):
    """test request results into OAILoadException"""

    # arrange
    data_path = os.path.join(str(ROOT), 'tests/resources/opendata/id_not_exist.xml')
    _req = mock_response(status_code=200,
                         headers={'Content-Type': 'text/xml;charset=UTF-8'},
                         data_path=data_path)
    mock_requests.return_value = _req

    # act
    with pytest.raises(LoadException) as exc:
        request_resource('http://foo.bar', Path())

    assert 'verb requires' in str(exc.value)


@unittest.mock.patch('requests.get')
def test_oai_load_exception_for_server_error(mock_504, tmp_path):
    """Ensure OAILoadException for Response status_code
    which indicates internal Server-Errors gets properly
    propagated upstream to caller.

    Because we're testing the very response status_code,
    any subsequent steps that usually require additional
    information for parsing the response content (like
    prime id) are not needed and thereforse just set
    with dummy identifier 'foo'
    """

    # arrange
    # arrange
    _req = mock_response(status_code=504)
    mock_504.return_value = _req
    record = df_r.Record('foo')
    _id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / _id
    store_dir = tmp_path / "STORE" / "dd" / _id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + _id + '.xml'
    request_kwargs = dict(headers={'User-Agent': 'Smith'})

    # act
    loader = OAILoader(local_dir, base_url=OAI_BASE_URL_OPENDATA,
                       group_images=key_images,
                       post_oai=extract_mets,
                       request_kwargs=request_kwargs)
    loader.store = LocalStore(store_dir, local_dir)

    # act
    with pytest.raises(Exception) as exc:
        loader.load(record.identifier, local_dst, 'foo')

    # assert
    assert exc.typename == 'ServerError'
    _msg = exc.value.args[0]
    assert _msg == "url 'opendata.uni-halle.de/oai/dd?verb=GetRecord&metadataPrefix=mets&identifier=foo' returned '504'"
