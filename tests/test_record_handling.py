"""Specification for IO API"""

import json
import os
import unittest.mock

from pathlib import Path

import pytest

import digiflow.digiflow_io as df_io
import digiflow.digiflow_metadata as df_md
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


def test_record_update_info_set_input():
    """Prevent TypeError for input info alike
    {'vd17': '3:607751D', 'mps': [(3.5, 304), (3.6, 455), (3.7, 184)], 
      'ocr_loss': {'n.a.', '00000838'}}
    """

    the_urn = "oai:opendata2.uni-halle.de:1516514412012/27399"
    record = df_r.Record(the_urn)
    the_info = {'vd17': '3:607751D',
                'mps': [(3.5, 304), (3.6, 455), (3.7, 184)], 
                'ocr_loss': {'n.a.', '00000838'}}
    record.info = the_info

    # act
    with pytest.raises(df_r.RecordDataException) as data_exc:
        json.dumps(record.dict())

    # assert
    assert record.info["ocr_loss"] == {"n.a.", "00000838"}
    assert "Object of type set is not JSON serializable" in data_exc.value.args[0]


def test_record_update_info_valid_input():
    """Prevent TypeError for info string alike
    {'vd17': '3:607751D', 'urn': 'urn:nbn:de:gbv:3:1-42926',
      'mps': [(3.5, 304), (3.6, 455), (3.7, 184)], 
      'ocr_loss': {'n.a.', '00000838'}, 'n_execs': '8'}
    """

    the_urn = "oai:opendata2.uni-halle.de:1516514412012/27399"
    record = df_r.Record(the_urn)
    the_info = {'vd17': '3:607751D', 'urn': 'urn:nbn:de:gbv:3:1-42926',
      'n_images_ocrable': 943, 'mps': [(3.5, 304), (3.6, 455), (3.7, 184)], 
      'ocr_loss': ['n.a.', '00000838'], 'n_execs': '8'}
    record.info = the_info

    # act
    serialized_info = json.dumps(record.dict())

    # assert
    assert '"ocr_loss": ["n.a.", "00000838"]' in serialized_info


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
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + the_id + '.xml'

    # act
    loader = df_io.OAILoader(local_dir, base_url='digitale.bibliothek.uni-halle.de/vd16/oai',
                             group_images=key_images,
                             post_oai=df_md.extract_mets)
    loader.store = df_io.LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst, 'md997508')

    # assert first download of 1 xml + 12 image resources
    assert number == 13
    assert mock_request_vd16_997508.call_count == 13
    assert os.path.isfile(str(local_dir / (the_id + ".xml")))
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
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + the_id + '.xml'

    # act
    loader = df_io.OAILoader(local_dir, base_url=OAI_BASE_URL_OPENDATA,
                             group_images=key_images,
                             post_oai=df_md.extract_mets)
    loader.store = df_io.LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst)

    # assert
    assert number == 12
    assert os.path.isdir(str(local_dir))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isfile(str(local_dir / (the_id + ".xml")))
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
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + the_id + '.xml'
    request_kwargs = dict(headers={'User-Agent': 'Smith'})

    # act
    loader = df_io.OAILoader(local_dir, base_url=OAI_BASE_URL_OPENDATA,
                             group_images=key_images,
                             post_oai=df_md.extract_mets,
                             request_kwargs=request_kwargs)
    loader.store = df_io.LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst)

    # assert
    assert number == 12
    assert os.path.isdir(str(local_dir))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isfile(str(local_dir / (the_id + ".xml")))
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
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "zd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    local_dst = str(local_dir) + '/' + the_id + '.xml'

    # act
    loader = df_io.OAILoader(local_dir, base_url=OAI_BASE_URL_ZD,
                             post_oai=df_md.extract_mets)
    loader.store = df_io.LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst)

    # assert
    assert number == 17
    assert os.path.isdir(str(local_dir))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isfile(str(local_dir / (the_id + ".xml")))
    assert os.path.isfile(str(local_dir / "MAX" / "16331052.jpg"))

    # check cache
    assert os.path.exists(str(store_dir))


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
def test_oai_load_exception_for_server_error(mock_504: unittest.mock.Mock, tmp_path):
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
    the_response_req = mock_response(status_code=504)
    mock_504.return_value = the_response_req
    record = df_r.Record('foo')
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = 'MAX'
    local_dst = str(local_dir) + '/' + the_id + '.xml'
    request_kwargs = dict(headers={'User-Agent': 'Smith'})

    # act
    loader = df_io.OAILoader(local_dir, base_url=OAI_BASE_URL_OPENDATA,
                             group_images=key_images,
                             post_oai=df_md.extract_mets,
                             request_kwargs=request_kwargs)
    loader.store = df_io.LocalStore(store_dir, local_dir)

    # act
    with pytest.raises(Exception) as exc:
        loader.load(record.identifier, local_dst, 'foo')

    # assert
    assert exc.typename == 'ServerError'
    a_msg = exc.value.args[0]
    assert a_msg == "opendata.uni-halle.de/oai/dd?verb=GetRecord&metadataPrefix=mets&identifier=foo status 504"
