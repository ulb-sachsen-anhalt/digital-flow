# -*- coding: utf-8 -*-

import ast
import json
import os
import pytest
import shutil
import sys
import uuid
from pathlib import (
    Path
)
from unittest import (
    mock
)
from json.decoder import (
    JSONDecodeError
)

import lxml.etree as ET

from digiflow import (
    write_xml_file,
    OAIRecord,
    OAIRecordCriteriaState,
    OAIRecordCriteriaDatetime,
    OAIRecordCriteriaIdentifier,
    OAIRecordCriteriaText,
    OAIFileSweeper,
    OAIRecordHandler,
    OAILoadException,
    OAILoadServerError,
    OAILoadClientError,
    F_IDENTIFIER,
    F_SPEC,
    F_DATESTAMP,
    F_STATE_INFO,
    F_STATE,
    F_STATE_TS,
    RECORD_STATE_MASK_FRAME,
    HEADER_MIGRATION,
    OAILoader,
    post_oai_extract_mets,
    LocalStore,
    send_mail,
    request_resource,
    transform_to_record,
    get_enclosed,
)

from .conftest import (
    TEST_RES
)

ROOT = Path(__file__).parents[1]

EXPORT_METS = 'export_mets.xml'

# some test constants
ID_737429 = '737429'
OAI_ID_737429 = 'oai:digital.bibliothek.uni-halle.de/hd:{}'.format(ID_737429)
OAI_SPEC_737429 = 'ulbhaldod'
CONTENT_TYPE_TXT = 'text/xml;charset=utf-8'
OAI_BASE_URL_VD16 = 'digitale.bibliothek.uni-halle.de/vd16/oai'
OAI_BASE_URL_ZD = 'digitale.bibliothek.uni-halle.de/zd/oai'
OAI_BASE_URL_OPENDATA = 'opendata.uni-halle.de/oai/dd'


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
    txt = '<parent><child name="foo">bar</child></parent>'
    xml_tree = ET.fromstring(txt)
    outpath = tmp_path / "write_foo.xml"
    write_xml_file(xml_tree, str(outpath))

    assert os.path.isfile(str(outpath))
    assert open(str(outpath)).read().startswith(
        '<?xml version="1.0" encoding="UTF-8"?>\n')


def test_write_xml_without_preamble(tmp_path):
    txt = '<parent><child name="foo">bar</child></parent>'
    xml_tree = ET.fromstring(txt)
    outpath = tmp_path / "write_foo.xml"
    write_xml_file(xml_tree, str(outpath), preamble=None)

    assert os.path.isfile(str(outpath))
    assert open(str(outpath)).read().startswith('<parent>\n')


@pytest.mark.parametrize(['urn', 'local_identifier'],
                         [
    ('oai:digital.bibliothek.uni-halle.de/hd:10595', '10595'),
    ('oai:digitale.bibliothek.uni-halle.de/vd18:9427342', '9427342'),
    ('oai:opendata.uni-halle.de:1981185920/34265', '1981185920_34265'),
    ('oai:dev.opendata.uni-halle.de:123456789/27949', '123456789_27949'),
])
def test_record_local_identifiers(urn, local_identifier):

    # act
    record = OAIRecord(urn)
    assert record.local_identifier == local_identifier


def fixture_request_results(*args, **kwargs):
    """
    Provide local copies for corresponding download request
    * dd/oai:opendata.uni-halle.de:1981185920/36020
    """
    the_url = args[0]
    the_headers = kwargs['headers'] if 'headers' in kwargs else {}
    result = mock.Mock()
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


@mock.patch("digiflow.requests.get")
def test_oai_load_vd16_with_localstore(mock_request_vd16_997508, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_vd16_997508.side_effect = fixture_request_results
    ident = 'oai:digitale.bibliothek.uni-halle.de/vd16:997508'
    record = OAIRecord(ident)
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
                       post_oai=post_oai_extract_mets)
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


@mock.patch("digiflow.requests.get")
def test_oai_load_opendata_with_localstore(
        mock_request_1981185920_36020, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_1981185920_36020.side_effect = fixture_request_results
    ident = 'oai:opendata.uni-halle.de:1981185920/36020'
    record = OAIRecord(ident)
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
                       post_oai=post_oai_extract_mets)
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


@mock.patch("digiflow.requests.get")
def test_oai_load_opendata_request_kwargs(
        mock_request_1981185920_36020, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_1981185920_36020.side_effect = fixture_request_results
    ident = 'oai:opendata.uni-halle.de:1981185920/36020'
    record = OAIRecord(ident)
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
                       post_oai=post_oai_extract_mets,
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


DEFAULT_HEADERS = "{}\t{}\t{}\t{}\t{}\t{}\n"\
    .format(F_IDENTIFIER, F_SPEC, F_DATESTAMP, F_STATE_INFO, F_STATE, F_STATE_TS)


def _write_datalist(path_data_list, data, headers=DEFAULT_HEADERS):
    with open(str(path_data_list), 'w') as handle:
        if headers:
            handle.write(headers)
        handle.writelines(data)


def test_invalid_input_data(tmp_path):
    """Invalid input data format raises an exception"""

    # arrange
    invalid_path_dir = tmp_path / 'invalid_data'
    invalid_path_dir.mkdir()
    invalid_path = invalid_path_dir / 'invalid.tsv'
    data = ["123\t456\t789\t0\n", "124\t457\t790\t1\n"]
    _write_datalist(invalid_path, data, headers=None)

    with pytest.raises(RuntimeError) as exc:
        OAIRecordHandler(invalid_path, data_fields=[F_IDENTIFIER, F_STATE])

    assert "invalid fields" in str(exc.value)


@pytest.fixture(name="valid_datasets")
def _fixture_valid_input_data(tmp_path):
    """
    provide valid input data
    * row [0] = 'IDENTIFIER' following oai-protocol
    * row [1] = 'SETSPEC' additional information on set and type (optional)
    * row [2] = 'CREATED' when item was created by legacy system
    * row [3] = 'STATE' migration state, 'n.a.' when open to migration
    * row [4] = 'FINISHED' datetime of last successfull migration run
    """
    valid_path_dir = tmp_path / 'valid_data'
    valid_path_dir.mkdir()
    valid_path = valid_path_dir / 'valid.tsv'
    data = [
        "oai:myhost.de/dod:123\tdod##book\t2009-11-03T13:20:32Z\tn.a.\tn.a.\tn.a.\n",
        "oai:myhost.de/dod:124\tdod##book\t2009-11-04T13:20:32Z\tn.a.\tn.a.\tn.a.\n"
    ]
    _write_datalist(valid_path, data)
    return str(valid_path)


def test_migrate_state_saved(valid_datasets):
    """
    Valid input data format enables to mark
    successfull outcomes by row nr.5 & nr. 6
    """

    # arrange
    handler = OAIRecordHandler(valid_datasets, mark_lock='ocr_done')
    a_set = handler.next_record()
    assert a_set.identifier == 'oai:myhost.de/dod:123'

    # act
    handler.save_record_state(a_set.identifier, state='ocr_done', INFO='444')

    # assert
    next_dataset = handler.next_record()
    assert next_dataset.identifier == 'oai:myhost.de/dod:124'

    # act
    # assert
    handler.save_record_state(next_dataset.identifier, 'ocr_done', INFO='555')

    # nothing left to do
    assert not handler.next_record()


@pytest.fixture(name="vl_datasets")
def _fixture_vl_datasets_input_data(tmp_path):
    """
    provide valid input data
    * row [0] = 'IDENTIFIER' following oai-protocol
    * row [1] = 'SETSPEC' additional information on set and type (optional)
    * row [2] = 'CREATED' when item was created by legacy system
    * row [3] = 'STATE' migration state, 'n.a.' when open to migration
    * row [4] = 'FINISHED' datetime of last successfull migration run
    """
    valid_path_dir = tmp_path / 'vl_datasets'
    valid_path_dir.mkdir()
    valid_path = valid_path_dir / 'vl_datasets.tsv'
    data = [
        'oai:menadoc.bibliothek.uni-halle.de/menalib:1416976\tmenalib\t2009-11-03T13:20:32Z\tn.a.\tn.a.\tn.a.\n',
        'oai:digitale.bibliothek.uni-halle.de/vd17:696\tpon##book\t2009-11-04T13:20:32Z\tn.a.\tn.a.\tn.a.\n'
    ]
    _write_datalist(valid_path, data)
    return str(valid_path)


def test_migration_dataset_vl_formats(vl_datasets):
    """
    Valid input data format enables to mark
    successfull outcomes by row nr.5 & nr. 6
    """

    # arrange
    handler = OAIRecordHandler(vl_datasets)
    a_set = handler.next_record()

    # assert
    assert handler.position == "0001/0002"
    assert a_set.local_identifier == '1416976'
    assert a_set.set == 'menalib'
    handler.save_record_state(a_set.identifier, 'busy')

    a_set = handler.next_record()
    assert handler.position == "0002/0002"
    assert a_set.local_identifier == '696'
    assert a_set.set == 'pon##book'


def test_migration_dataset_vl_info_stays(vl_datasets):
    """
    Valid input data format enables to mark
    successfull outcomes by row nr.5 & nr. 6
    """

    # arrange
    handler = OAIRecordHandler(vl_datasets)
    a_set = handler.next_record()

    # assert
    assert a_set.local_identifier == '1416976'
    handler.save_record_state(a_set.identifier, 'metadata_done', INFO='123,ger')
    handler.save_record_state(a_set.identifier, 'migration_done')

    a_set = handler.next_record(state='migration_done')
    assert a_set.local_identifier == '1416976'
    with open(vl_datasets) as reader:
        lines = reader.readlines()
    assert lines[1].split('\t')[3] == '123,ger'


def test_dataset_cannot_find_entry(vl_datasets):
    """Behavior if want to store with unknown identifier"""

    # arrange
    handler = OAIRecordHandler(vl_datasets)

    # act
    with pytest.raises(RuntimeError) as exc:
        handler.save_record_state('foo')

    # assert
    assert 'No Record for foo' in str(exc.value)


def test_statelist_ocr_hdz(tmp_path):
    """Behavior of state lists for ocr pipeline"""

    # arrange
    path_state_list = tmp_path / 'ocr_list'
    first_row = "{}\t{}\t{}\n".format(F_IDENTIFIER, F_STATE, F_STATE_TS)
    data = [
        'oai:digitale.bibliothek.uni-halle.de/zd:123\tn.a.\tn.a.\n'
    ]
    _write_datalist(path_state_list, data, first_row)
    headers = [F_IDENTIFIER, F_STATE, F_STATE_TS]
    handler = OAIRecordHandler(
        path_state_list,
        data_fields=headers,
        transform_func=lambda r: r)

    # act
    record = handler.next_record()
    assert handler.position == '0001/0001'
    assert record[F_IDENTIFIER] == 'oai:digitale.bibliothek.uni-halle.de/zd:123'
    handler.save_record_state(record[F_IDENTIFIER])

    # assert no next open record
    assert not handler.next_record()

    # assert there is a record next with default state 'lock'
    assert handler.next_record('busy')


# record list header data
OAI_LIST_HEADER = "{}\t{}\t{}\t{}\t{}\n".format(
    F_IDENTIFIER, F_DATESTAMP, F_STATE_INFO, F_STATE, F_STATE_TS)


@pytest.fixture(name="oai_record_list")
def _fixture_oai_record_list(tmp_path):
    path_state_list = tmp_path / 'ocr_list'
    data = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853011\t2015-08-25T20:00:35Z\tn.a.\tocr_skip\t2021-08-03_15:03:56\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:17320046\t2021-09-01T15:25:43Z\t17320046,issue,ger,20\tupload_done\t2021-09-09_22:57:45\n"
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tocr_skip\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tocr_skip\t2021-08-03_15:20:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\t2015-08-25T20:00:35Z\t9510507,issue,[('9510507', 'issue'), ('n.a.', 'section'), ('8849883', 'year'), ('9059307', 'newspaper')],,None\tocr_done\t2021-08-03_16:44:54\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510508\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    _write_datalist(path_state_list, data, OAI_LIST_HEADER)
    return path_state_list


def _morph(row):
    r = {}
    r[F_IDENTIFIER] = row[F_IDENTIFIER]
    r[F_STATE] = row[F_STATE]
    r[F_STATE_TS] = row[F_STATE_TS]
    return r


# OAI record list default headers
headers = [F_IDENTIFIER, F_DATESTAMP, F_STATE_INFO, F_STATE, F_STATE_TS]


def test_statelist_use_n_a_properly(oai_record_list):
    """Behavior of state lists for ocr pipeline"""

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)

    # act
    record = handler.next_record()
    assert handler.position == '0006/0006'
    assert record[F_IDENTIFIER] == 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    handler.save_record_state(record[F_IDENTIFIER])

    # assert no next open record
    assert not handler.next_record()

    # assert there is a record with default state 'lock'
    assert handler.next_record('busy')


def fixture_request_vls_zd1_16359609(*args, **kwargs):
    """
    Provide local copies for corresponding download request
    Data: oai:digitale.bibliothek.uni-halle.de/zd:16359609
    """
    the_url = args[0]
    the_headers = kwargs['headers'] if 'headers' in kwargs else {}
    result = mock.Mock()
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


@mock.patch("digiflow.requests.get")
def test_oai_load_vls_zd1_with_ocr(mock_request, tmp_path):
    """Behavior of state lists for ocr pipeline"""

    # arrange
    """test oai loader implementation for opendata"""

    # arrange
    mock_request.side_effect = fixture_request_vls_zd1_16359609
    ident = 'oai:digitale.bibliothek.uni-halle.de/zd:16359609'
    record = OAIRecord(ident)
    _id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / _id
    store_dir = tmp_path / "STORE" / "zd" / _id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    local_dst = str(local_dir) + '/' + _id + '.xml'

    def post_write_mets(the_self, the_data):
        """Just extract METS from OAI body"""
        xml_root = ET.fromstring(the_data)
        write_xml_file(xml_root, the_self.path_mets, preamble=None)
        return the_self.path_mets

    # act
    loader = OAILoader(local_dir, base_url=OAI_BASE_URL_ZD,
                       post_oai=post_write_mets)
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


@mock.patch('digiflow.digiflow_io.get_smtp_server')
def test_send_mail(mock_smtp):
    """test sending mail"""

    # arrange
    random_message = uuid.uuid4().hex

    # act
    mess = send_mail(
        subject='test',
        message=random_message,
        sender='test@example.com',
        recipients='me@example.de')

    # assert
    assert random_message in mess
    assert 'notification' in mess
    assert mock_smtp.called


def test_record_state_list_set_state_from(oai_record_list):
    """Behavior for changing state matching state and start time"""

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)
    c_state = OAIRecordCriteriaState('ocr_skip')
    c_from = OAIRecordCriteriaDatetime(dt_from='2021-08-03_15:03:56')

    # pre-check
    record1 = handler.next_record()
    assert record1[F_IDENTIFIER].endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_from], dry_run=False)

    # assert next open record changed
    record2 = handler.next_record()
    assert record2[F_IDENTIFIER].endswith('8853011')
    # assert we handled 3 records
    assert outcome == 3


def test_record_datestamp(oai_record_list):
    """Check if proper datestamp gets picked"""

    # arrange
    _handler = OAIRecordHandler(oai_record_list)

    # act
    _record = _handler.next_record()

    # assert
    assert _record.identifier == 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    assert _record.local_identifier == '9510508'
    assert _record.date_stamp == '2015-08-25T20:00:35Z'


def test_record_get_fullident(oai_record_list):
    """Check if proper datestamp gets picked"""

    # arrange
    _ident_urn = 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    _handler = OAIRecordHandler(oai_record_list)

    # act
    _record = _handler.get(_ident_urn)

    # assert
    assert _record.identifier == _ident_urn
    assert _record.date_stamp == '2015-08-25T20:00:35Z'


def test_record_get_partialident(oai_record_list):
    """Check if proper datestamp gets picked
    if just the latest segment (aka local
    identifier) has been provided"""

    # arrange
    _ident_urn = 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    _handler = OAIRecordHandler(oai_record_list)

    # act
    _record_exact = _handler.get('9510508')
    _record_fuzzy = _handler.get('9510508', exact_match=False)

    # assert
    assert not _record_exact
    assert _record_fuzzy.identifier == _ident_urn
    assert _record_fuzzy.date_stamp == '2015-08-25T20:00:35Z'


def test_record_get_non_existent(oai_record_list):
    """What happens if no match found?"""

    # arrange
    _handler = OAIRecordHandler(oai_record_list)

    # act
    assert not _handler.get('9510509')


def test_record_state_list_set_state_from_dry_run(oai_record_list):
    """Behavior of dry run"""

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)
    c_state = OAIRecordCriteriaState('ocr_skip')
    c_from = OAIRecordCriteriaDatetime(dt_from='2021-08-03_15:03:56')

    # pre-check
    record1 = handler.next_record()
    assert record1[F_IDENTIFIER].endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_from], dry_run=True)

    # assert no change of date
    record2 = handler.next_record()
    assert record2[F_IDENTIFIER].endswith('9510508')
    assert outcome == 3


def test_record_state_list_set_state_from_dry_run_verbose(oai_record_list, capsys):
    """Behavior of dry run"""

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)
    c_state = OAIRecordCriteriaState('ocr_skip')
    c_from = OAIRecordCriteriaDatetime(dt_from='2021-08-03_15:03:56')

    # pre-check
    record1 = handler.next_record()
    assert record1[F_IDENTIFIER].endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_from], dry_run=True, verbose=True)

    # assert
    assert outcome == 3
    _std_out = capsys.readouterr().out
    assert 'IDENTIFIER\tCREATED\tINFO\tSTATE\tSTATE_TIME' in _std_out
    assert 'oai:digitale.bibliothek.uni-halle.de/zd:8853012' in _std_out


def test_record_state_list_rewind_state_upload(oai_record_list):
    """Behavior for changing state"""

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)
    c_state = OAIRecordCriteriaState('upload_done')

    # pre-check
    record1 = handler.next_record()
    assert record1[F_IDENTIFIER].endswith('9510508')

    # act
    handler.states(criterias=[c_state], dry_run=False)
    assert handler.next_record()[F_IDENTIFIER].endswith('17320046')

    # assert next open record changed
    h2 = OAIRecordHandler(
        oai_record_list, data_fields=headers, transform_func=_morph)
    assert h2.next_record()[F_IDENTIFIER].endswith('17320046')


def test_record_set_some_other_state(oai_record_list):
    """Behavior for changing state"""

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)
    c_state = OAIRecordCriteriaState('upload_done')

    # pre-check
    record1 = handler.next_record()
    assert record1[F_IDENTIFIER].endswith('9510508')

    # act
    handler.states(
        criterias=[c_state], set_state='metadata_read', dry_run=False)
    assert not handler.next_record(state='upload_done')

    # assert next open record changed
    h2 = OAIRecordHandler(
        oai_record_list, data_fields=headers, transform_func=_morph)
    assert h2.next_record(state='metadata_read')[F_IDENTIFIER].endswith('17320046')


def test_record_list_time_range(oai_record_list):
    """Behavior for setting state within time range"""

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)
    c_state = OAIRecordCriteriaState('ocr_skip')
    c_range = OAIRecordCriteriaDatetime(dt_from='2021-08-03_15:10:00',
                                        dt_to='2021-08-03_15:20:00')

    # pre-check
    record1 = handler.next_record()
    assert record1[F_IDENTIFIER].endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_range], dry_run=False)
    assert outcome == 1

    # assert next open record changed
    record2 = handler.next_record()
    assert record2[F_IDENTIFIER].endswith('8853012')


def test_record_handler_merge_plain(tmp_path):
    """Behavior if merging 2 lists"""

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    _write_datalist(path_oai_list1, data1, OAI_LIST_HEADER)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\t2015-08-25T20:00:35Z\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    _write_datalist(path_oai_list2, data2, OAI_LIST_HEADER)
    handler = OAIRecordHandler(
        path_oai_list1,
        data_fields=headers,
        transform_func=_morph)

    # assert original next == first record with identifier 8853012
    assert handler.next_record()[F_IDENTIFIER].endswith('8853012')

    # act
    merge_result = handler.merges(path_oai_list2, dry_run=False)

    # list1 and list2 contain same records
    assert merge_result['matches'] == 2
    # list2 didn't contain any records *not* included in list1
    assert merge_result['misses'] == 0
    # two records have been merged
    assert merge_result['merges'] == 2
    # no new records
    assert merge_result['appendeds'] == 0
    # next must have changed, since 'n.a.' was merged with 'upload_done'
    assert handler.next_record()[F_IDENTIFIER].endswith('8853013')
    # state from record with ident '8853012' is now 'upload_done'
    criterias = [OAIRecordCriteriaIdentifier(
        '8853012'), OAIRecordCriteriaState('upload_done')]
    assert handler.states(criterias=criterias) == 1


def test_record_handler_merge_only_done(tmp_path):
    """Behavior if merging 2 lists with specific requirement"""

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    _write_datalist(path_oai_list1, data1, OAI_LIST_HEADER)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\t2015-08-25T20:00:35Z\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    _write_datalist(path_oai_list2, data2, OAI_LIST_HEADER)
    handler = OAIRecordHandler(
        path_oai_list1,
        data_fields=headers,
        transform_func=_morph)

    # act
    merge_result = handler.merges(path_oai_list2, dry_run=False, other_require_state='upload_done')

    # list1 and list2 contain same records
    assert merge_result['matches'] == 2
    # list2 didn't contain any records *not* included in list1
    assert merge_result['misses'] == 0
    # only the required record was merged
    assert merge_result['merges'] == 1
    # no new records
    assert merge_result['appendeds'] == 0
    # next must have changed, since 'n.a.' was merged with 'upload_done'
    assert handler.next_record()[F_IDENTIFIER].endswith('8853013')
    # state from record with ident '8853012' is now 'upload_done'
    criterias = [OAIRecordCriteriaIdentifier(
        '8853012'), OAIRecordCriteriaState('upload_done')]
    assert handler.states(criterias=criterias) == 1


def test_record_handler_merge_ignore_failure(tmp_path):
    """Behavior if merging 2 lists but ignore failures"""

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    _write_datalist(path_oai_list1, data1, OAI_LIST_HEADER)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\t2015-08-25T20:00:35Z\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    _write_datalist(path_oai_list2, data2, OAI_LIST_HEADER)
    handler = OAIRecordHandler(
        path_oai_list1,
        data_fields=headers,
        transform_func=_morph)

    # act
    merge_result = handler.merges(path_oai_list2, dry_run=False, other_ignore_state='upload_done')

    # list1 and list2 contain same records
    assert merge_result['matches'] == 2
    # list2 didn't contain records *not* included in list1
    assert merge_result['misses'] == 0
    # only the required record was merged
    assert merge_result['merges'] == 1
    # no new records
    assert merge_result['appendeds'] == 0
    # next is still first record
    assert handler.next_record()[F_IDENTIFIER].endswith('8853012')
    # state from record with ident '9510507' is now 'ocr_fail'
    criterias = [OAIRecordCriteriaIdentifier(
        '9510507'), OAIRecordCriteriaState('ocr_fail')]
    assert handler.states(criterias=criterias) == 1


def test_record_handler_merge_larger_into_smaller_dry_run(tmp_path):
    """Behavior if larger list gets merged into smaller.
    Question: How to deal with unknown dataset?
    """

    # arrange
    path_oai_list_a = tmp_path / 'oai_list_a'
    data = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n"
    ]
    _write_datalist(path_oai_list_a, data, OAI_LIST_HEADER)
    handler = OAIRecordHandler(
        path_oai_list_a,
        data_fields=headers,
        transform_func=_morph)
    path_oai_list_b = tmp_path / 'oai_list_b'
    data = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    _write_datalist(path_oai_list_b, data, OAI_LIST_HEADER)

    # assert original next == first record with identifier 8853012
    assert handler.next_record(state='upload_done')[
        F_IDENTIFIER].endswith('8853012')

    # act: must set ignore_state=None,
    # otherwise other list's records will be
    # completely ignored
    result = handler.merges(path_oai_list_b, other_ignore_state=None)

    # first record from self saved as-it-was, no merge
    assert result['merges'] == 0
    # second record from other not found in self, therefore missed
    assert result['misses'] == 1
    # second record from other also finally appended
    assert result['appendeds'] == 1

    # we still expected only '1' records to be in handler list a
    assert handler.total_len == 1


def test_record_handler_merge_larger_into_smaller_hot_run_subsequent(tmp_path):
    """Two lists merged into one list with two records.
    *Originally* this did only work because
    variable 'self_record' was *already* initialized by
    first run - although resulted into inconsistent data
    with resulting list containing 2x record no.1!"""

    # arrange
    path_oai_list_a = tmp_path / 'oai_list_a'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n"
    ]
    _write_datalist(path_oai_list_a, data1, OAI_LIST_HEADER)
    handler = OAIRecordHandler(
        path_oai_list_a,
        data_fields=headers,
        transform_func=_morph)
    path_oai_list_b = tmp_path / 'oai_list_b'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    _write_datalist(path_oai_list_b, data2, OAI_LIST_HEADER)

    # act: must *not* set ignore_state=None,
    # otherwise first record from second list erases
    # existing data from list a
    results = handler.merges(path_oai_list_b, dry_run=False)

    # no merge since state 'n.a.' from list b
    # first record of list b ignored
    # to preserve existing data from list a
    assert results['merges'] == 0
    assert results['ignores'] == 1
    assert results['appendeds'] == 1
    # now we expected '2' records to be in handler list
    assert handler.total_len == 2
    # now this is the first result record ...
    assert handler.next_record(state='upload_done')[F_IDENTIFIER].endswith('8853012')
    # ... and this shall be second record by now
    assert handler.next_record()[F_IDENTIFIER].endswith('8853013')


def test_record_handler_merge_larger_into_smaller_hot_run_inverse(tmp_path):
    """Ensure following Error is gone:
    'UnboundLocalError: local variable 'self_record' referenced before assignment'

    Yielded because record 1 from list 2 is unknown.
    """

    # arrange
    path_oai_list_a = tmp_path / 'oai_list_a'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n"
    ]
    _write_datalist(path_oai_list_a, data1, OAI_LIST_HEADER)
    handler = OAIRecordHandler(
        path_oai_list_a,
        data_fields=headers,
        transform_func=_morph)
    path_oai_list_b = tmp_path / 'oai_list_b'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
    ]
    _write_datalist(path_oai_list_b, data2, OAI_LIST_HEADER)

    # act:
    # ignore_state=None to overwrite
    # record 1 from list a (=merge)
    results = handler.merges(path_oai_list_b, dry_run=False, other_ignore_state=None)
    assert results['merges'] == 1
    assert results['ignores'] == 0
    assert results['appendeds'] == 1

    # two records have been merged
    # now we expected '2' records to be in handler list
    assert handler.total_len == 2
    # now this is the first result record ...
    record_012 = handler.next_record()
    assert record_012[F_IDENTIFIER].endswith('8853012')
    # mark state record 1
    handler.save_record_state(record_012[F_IDENTIFIER], state='foo_bar')
    # ... this shall be second record by now
    assert handler.next_record()[F_IDENTIFIER].endswith('8853013')


def test_record_handler_merge_cross(tmp_path):
    """
    Behavior merging 2 lists with cross-different
    info, with info from list 1 must be preserved
    but new info from list 2 must be integrated
    """

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
    ]
    _write_datalist(path_oai_list1, data1, OAI_LIST_HEADER)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\t2015-08-25T20:00:35Z\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    _write_datalist(path_oai_list2, data2, OAI_LIST_HEADER)
    handler = OAIRecordHandler(
        path_oai_list1,
        data_fields=headers,
        transform_func=_morph)

    # assert original next == first record with identifier 8853012
    assert handler.next_record()[F_IDENTIFIER].endswith('8853013')

    # act
    results = handler.merges(path_oai_list2, dry_run=False)

    # act
    assert results['merges'] == 1  # because info got merged from 2 record
    assert results['ignores'] == 1  # nothing missed, because all idents present
    assert not results['appendeds']  # no new, because all idents present
    assert not handler.next_record(state='n.a.')    # by now no more open records


def test_records_frame_with_start(oai_record_list):

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)

    # act
    new_path = handler.frame(2)

    # assert
    assert os.path.exists(new_path)
    assert os.path.basename(new_path) == 'ocr_list_02_06.csv'

    frame_handler = OAIRecordHandler(
        new_path,
        data_fields=headers,
        transform_func=_morph)

    # ensure that only first records is set to 'other_load'
    # all other records are within frame
    c_state = OAIRecordCriteriaState('other_load')
    assert frame_handler.states([c_state]) == 1


def test_records_frame_range(oai_record_list):

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        data_fields=headers,
        transform_func=_morph)

    # act
    new_path = handler.frame(2, 2)

    # assert
    assert os.path.exists(new_path)
    assert os.path.basename(new_path) == 'ocr_list_02_03.csv'

    frame_handler = OAIRecordHandler(
        new_path,
        data_fields=headers,
        transform_func=_morph)

    # ensure that by now 4 records are set to 'other_load'
    # first record and records 4,5,6
    c_state = OAIRecordCriteriaState('other_load')
    assert frame_handler.states([c_state]) == 4


def test_records_default_header_from_file(oai_record_list):

    # arrange
    handler = OAIRecordHandler(
        oai_record_list,
        transform_func=_morph)

    # act
    new_path = handler.frame(3)

    # assert
    assert os.path.exists(new_path)
    assert os.path.basename(new_path) == 'ocr_list_03_06.csv'

    frame_handler = OAIRecordHandler(
        new_path,
        transform_func=_morph)

    # ensure that by now 4 records are set to 'other_load'
    # first + second record only
    c_state = OAIRecordCriteriaState(RECORD_STATE_MASK_FRAME)
    assert frame_handler.states([c_state]) == 2


def mock_response(**kwargs):
    """Create custum mock object"""

    _response = mock.MagicMock()
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


@mock.patch('requests.get')
def test_response_404(mock_requests):
    """test request ends up with 417"""

    # arrange
    _req = mock_response(status_code=417)
    mock_requests.return_value = _req

    # act
    with pytest.raises(OAILoadException) as exc:
        request_resource('http://foo.bar', Path())

    # assert
    assert exc.typename == 'OAILoadClientError'
    assert "url 'http://foo.bar' returned '417'" == exc.value.message


@mock.patch('requests.get')
def test_response_200_with_error_content(mock_requests):
    """test request results into OAILoadException"""

    # arrange
    data_path = os.path.join(str(ROOT), 'tests/resources/opendata/id_not_exist.xml')
    _req = mock_response(status_code=200, 
                         headers= {'Content-Type': 'text/xml;charset=UTF-8'},
                         data_path=data_path)
    mock_requests.return_value = _req

    # act
    with pytest.raises(OAILoadException) as exc:
        request_resource('http://foo.bar', Path())

    assert 'verb requires' in str(exc.value)


def test_records_sample_zd1_post_ocr():

    path_list = os.path.join(str(ROOT), 'tests', 'resources',
                             'vls', 'oai-urn-zd1-sample70k.tsv')
    assert os.path.isfile(path_list)

    # arrange
    handler = OAIRecordHandler(
        path_list)
    crit1 = OAIRecordCriteriaDatetime(dt_from='2021-10-16_09:45:00')

    # assert
    assert 1 == handler.states([crit1])
    crit2 = OAIRecordCriteriaState('other_load')
    assert 10 == handler.states([crit2])


def test_recordcriteria_with_created_datetime_format():
    """Expect 12 records to have a CREATED datetime before 15:26:00"""

    path_list = os.path.join(str(ROOT), 'tests', 'resources',
                             'vls', 'oai-urn-zd1-sample70k.tsv')
    assert os.path.isfile(path_list)

    # arrange
    handler = OAIRecordHandler(
        path_list)
    crit1 = OAIRecordCriteriaDatetime(
        dt_field='CREATED',
        dt_to='2021-09-01T15:26:00Z',
        dt_format='%Y-%m-%dT%H:%M:%SZ')

    # assert
    assert 12 == handler.states([crit1])


def test_record_handler_search_info(tmp_path):
    """
    Behavior searching special textual content in a specific column
    """

    # arrange
    path_oai_list1 = tmp_path / 'oai-list'
    the_data = [
        ("oai:digitale.bibliothek.uni-halle.de/vd18:1178220\tulbhalvd18##book\t2009-11-23T10:51:32Z\t"
         "683567713,Aa,vd18#10198547/urn#urn:nbn:de:gbv:3:1-114513,[],lat,['AB 71 B 3/g, 23'],582 errs:{'no_publ_place': '683567713'},no colorchecker\t"
         "fail\t2021-12-08_13:08:14\n"),
        ("oai:digitale.bibliothek.uni-halle.de/vd18:1177464\tulbhalvd18##book\t2009-11-24T07:23:00Z\t"
         "30959913X,Aa,vd18#1009007X/gbv#30959913X/urn#urn:nbn:de:gbv:3:1-114858,[],ger,['AB 95878'],472 errs:no colorchecker\t"
         "fail\t2021-12-08_13:10:52\n"),
        ("oai:digitale.bibliothek.uni-halle.de/vd18:1178423\tulbhalvd18##book\t2009-11-18T08:39:21Z\t"
         "242994199,Aa,vd18#10084061/gbv#242994199/urn#urn:nbn:de:gbv:3:1-114422,[],fre#ger,['AB 39 12/k, 21'],479,cc\t"
         "migration_done\t2021-12-08_12:36:15\n")
    ]
    _write_datalist(path_oai_list1, the_data)
    handler = OAIRecordHandler(
        path_oai_list1,
        data_fields=HEADER_MIGRATION,
        transform_func=_morph)
    crit1 = OAIRecordCriteriaText('no colorchecker')
    crit2 = OAIRecordCriteriaText('no_publ_place')

    # assert original next == nothing open
    assert not handler.next_record()
    # records contain token "no colorchecker" in their INFO ...
    assert 2 == handler.states([crit1])
    # ... but only one record matches both
    assert 1 == handler.states([crit1, crit2])

    # act
    affected = handler.states([crit1, crit2], dry_run=False)
    assert affected == 1
    assert handler.next_record()    # by now one open record


def test_record_handler_quotation_broken(tmp_path):
    """
    Fix behavior when encountering data with
    broken format from a mixture of single and double quotes

    originates from ODEM project csv list
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'oai-records-opendata-vd18-sample-broken.csv')
    path_oai_list1 = tmp_path / 'oai-list.csv'
    shutil.copy(path_list_sample, path_oai_list1)
    handler = OAIRecordHandler(path_oai_list1, transform_func=transform_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')    
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    with pytest.raises(JSONDecodeError) as _decode_err:
        json.loads(_next_rec.info)
    assert 'Extra data: line 1 column 7 (char 6)' in _decode_err.value.args[0]


def test_record_handler_quotation_mixture_json(tmp_path):
    """
    Fix behavior when encountering data which
    has been fixed manually resulting in a mixture
    of double and single quotes

    originates from ODEM project csv list
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'oai-records-opendata-vd18-sample-mixture.csv')
    path_oai_list1 = tmp_path / 'oai-list.csv'
    shutil.copy(path_list_sample, path_oai_list1)
    handler = OAIRecordHandler(path_oai_list1, transform_func=transform_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')    
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    with pytest.raises(JSONDecodeError) as _decode_err:
        json.loads(_next_rec.info)
    assert 'Extra data: line 1 column 7 (char 6)' in _decode_err.value.args[0]

def test_record_handler_quotation_fixed_json(tmp_path):
    """
    Fix behavior when encountering data which
    has been fixed manually resulting in a mixture
    of double and single quotes

    originates from ODEM project csv list
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'oai-records-opendata-vd18-sample-fixed.csv')
    path_oai_list1 = tmp_path / 'oai-list.csv'
    shutil.copy(path_list_sample, path_oai_list1)
    handler = OAIRecordHandler(path_oai_list1, transform_func=transform_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')    
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    _info_token = get_enclosed(_next_rec.info)
    _last_info = json.loads(_info_token)['info']
    assert 'processing https://opendata.uni-halle.de/retrieve/b7f7f81d-e65f-4c7d-95c6-7384b184c6a9/00001051.jpg' in _last_info


def test_record_handler_quotation_fixed_ast(tmp_path):
    """
    Fix behavior when encountering data which
    has been fixed manually resulting in a mixture
    of double and single quotes

    originates from ODEM project csv list
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'oai-records-opendata-vd18-sample-fixed.csv')
    path_oai_list1 = tmp_path / 'oai-list.csv'
    shutil.copy(path_list_sample, path_oai_list1)
    handler = OAIRecordHandler(path_oai_list1, transform_func=transform_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')    
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    _info_token = get_enclosed(_next_rec.info)
    _last_info = ast.literal_eval(_info_token)['info']
    assert 'processing https://opendata.uni-halle.de/retrieve/b7f7f81d-e65f-4c7d-95c6-7384b184c6a9/00001051.jpg' in _last_info


def test_record_handler_quotation_mixture_ast(tmp_path):
    """
    Fix behavior when encountering data which
    has been fixed manually resulting in a mixture
    of double and single quotes that shall be parsed with
    standard python eval from ast module which should
    work if the key "info" is double-quotes as well
    as the total information string which might contain
    single-quotes

    originates from ODEM project csv list
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'oai-records-opendata-vd18-sample-mixture.csv')
    path_oai_list1 = tmp_path / 'oai-list.csv'
    shutil.copy(path_list_sample, path_oai_list1)
    handler = OAIRecordHandler(path_oai_list1, transform_func=transform_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')    
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    _info_token = get_enclosed(_next_rec.info)
    _last_info = ast.literal_eval(_info_token)['info']
    assert "processing 'https://opendata.uni-halle.de/retrieve/b7f7f81d-e65f-4c7d-95c6-7384b184c6a9/00001051.jpg'" in _last_info


@pytest.mark.skipif(sys.version_info >= (3,10), reason="Specific to Python <3.10")
def test_record_handler_quotation_mixture_ast_639763(tmp_path):
    """
    Behavior when encountering data like this:

    originates from ODEM project csv list

    Please note: changed from python 3.8 to 3.10
        therefore another testcase
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'oai-records-opendata-vd18-sample-mixture.csv')
    path_oai_list1 = tmp_path / 'oai-list.csv'
    shutil.copy(path_list_sample, path_oai_list1)
    handler = OAIRecordHandler(path_oai_list1, transform_func=transform_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_busy')    
    _info_token = get_enclosed(_next_rec.info)
    with pytest.raises(SyntaxError) as _decode_err:
        ast.literal_eval(_info_token)
    assert 'invalid syntax' in _decode_err.value.args[0]


@pytest.mark.skipif(sys.version_info < (3,10), 
                    reason="Specific to Python >= 3.10")
def test_record_handler_quotation_mixture_ast_639763_python_310(tmp_path):
    """
    Behavior when encountering data like this:

    originates from ODEM project csv list

    Please note: changed from python 3.8 to 3.10
        therefore another testcase
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'oai-records-opendata-vd18-sample-mixture.csv')
    path_oai_list1 = tmp_path / 'oai-list.csv'
    shutil.copy(path_list_sample, path_oai_list1)
    handler = OAIRecordHandler(path_oai_list1, transform_func=transform_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_busy')    
    _info_token = get_enclosed(_next_rec.info)
    with pytest.raises(SyntaxError) as _decode_err:
        ast.literal_eval(_info_token)
    assert "unmatched '}'" in _decode_err.value.args[0]


@mock.patch('requests.get')
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
    record = OAIRecord('foo')
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
                       post_oai=post_oai_extract_mets,
                       request_kwargs=request_kwargs)
    loader.store = LocalStore(store_dir, local_dir)

    # act
    with pytest.raises(Exception) as exc:
        loader.load(record.identifier, local_dst, 'foo')

    # assert
    assert exc.typename == 'OAILoadServerError'
    _msg = exc.value.args[0]
    assert _msg == "url 'opendata.uni-halle.de/oai/dd?verb=GetRecord&metadataPrefix=mets&identifier=foo' returned '504'"
