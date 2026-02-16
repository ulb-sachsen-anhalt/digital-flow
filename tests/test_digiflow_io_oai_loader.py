"""Test specification for OAILoader class"""

import os
import unittest.mock

from pathlib import Path

import pytest

import digiflow.digiflow_io as df_io
import digiflow.digiflow_metadata as df_md
import digiflow.record as df_r

from .conftest import mock_response

ROOT = Path(__file__).parents[1]

# OAI test constants
CONTENT_TYPE_TXT = "text/xml;charset=utf-8"
OAI_BASE_URL_VD16 = "digitale.bibliothek.uni-halle.de/vd16/oai"
OAI_BASE_URL_ZD = "digitale.bibliothek.uni-halle.de/zd/oai"
OAI_BASE_URL_OPENDATA = "opendata.uni-halle.de/oai/dd"

# pylint: disable=c-extension-no-member, line-too-long


def fixture_request_results(*args, **kwargs):
    """
    Provide local copies for corresponding download request
    * dd/oai:opendata.uni-halle.de:1981185920/36020
    """
    the_url = args[0]
    the_headers = kwargs["headers"] if "headers" in kwargs else {}
    result = unittest.mock.Mock()
    result.status_code = 200
    result.headers = {"Content-Type": "image/jpeg"}
    if the_headers:
        for k, v in the_headers.items():
            result.headers[k] = v
        # , 'User-Agent': the_headers['User-Agent']}
    max_image_dir = os.path.join(str(ROOT), "tests/resources/vls/monography/737429/MAX")
    # this one is the METS/MODS
    if the_url.endswith("36020"):
        result.headers["Content-Type"] = CONTENT_TYPE_TXT
        data_path = os.path.join(
            str(ROOT), "tests/resources/opendata/1981185920_36020.oai.xml"
        )
        with open(data_path, "rb") as xml:
            result.content = xml.read()
    elif the_url.endswith("997508"):
        result.headers["Content-Type"] = CONTENT_TYPE_TXT
        data_path = os.path.join(str(ROOT), "tests/resources/vls/vd16-oai-997508.xml")
        with open(data_path, "rb") as xml:
            result.content = xml.read()
    else:
        with open(max_image_dir + "/737434.jpg", "rb") as img:
            result.content = img.read()
    return result


def mock_request_vls_zd1_16359609(*args, **kwargs):
    """
    Provide local copies for corresponding download request
    Data: oai:digitale.bibliothek.uni-halle.de/zd:16359609
    """
    the_url = args[0]
    the_headers = kwargs["headers"] if "headers" in kwargs else {}
    result = unittest.mock.Mock()
    result.status_code = 200
    result.headers = {"Content-Type": CONTENT_TYPE_TXT}
    if the_headers:
        for k, v in the_headers.items():
            result.headers[k] = v
        # , 'User-Agent': the_headers['User-Agent']}
    max_image_dir = os.path.join(str(ROOT), "tests/resources/vls/monography/737429/MAX")
    # this one is the METS/MODS
    if the_url.endswith("16359609"):
        data_path = os.path.join(
            str(ROOT), "tests/resources/vls/zd/zd1-16359609.mets.xml"
        )
        with open(data_path, encoding="utf-8") as xml:
            result.content = xml.read()
    elif "download/webcache/" in the_url:
        result.headers = {"Content-Type": "image/jpeg"}
        with open(max_image_dir + "/737434.jpg", "rb") as img:
            result.content = img.read()
    elif "download/fulltext/" in the_url:
        alto_file = os.path.join(str(ROOT), "tests/resources/vls/zd/")
        with open(alto_file + "zd1-alto-16331001.xml", encoding="utf-8") as hndl:
            result.content = hndl.read().encode()
    return result


# ============================================================================
# Tests for OAILoader.load method
# ============================================================================


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_vd16_with_localstore(mock_request_vd16_997508, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_vd16_997508.side_effect = fixture_request_results
    ident = "oai:digitale.bibliothek.uni-halle.de/vd16:997508"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = "MAX"
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url="digitale.bibliothek.uni-halle.de/vd16/oai",
        group_images=key_images,
        post_oai=df_md.extract_mets,
    )
    loader.store = df_io.LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, local_dst, "md997508")

    # assert first download of 1 xml + 12 image resources
    assert number == 13
    assert mock_request_vd16_997508.call_count == 13
    assert os.path.isfile(str(local_dir / (the_id + ".xml")))
    assert os.path.isfile(str(local_dir / "MAX" / "1019932.jpg"))

    # ensure no subsequent re-load took place
    assert not loader.load(record.identifier, local_dst, "md997508")
    assert mock_request_vd16_997508.call_count == 13


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_opendata_with_localstore(mock_request_1981185920_36020, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_1981185920_36020.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = "MAX"
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA,
        group_images=key_images,
        post_oai=df_md.extract_mets,
    )
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
def test_oai_load_opendata_request_kwargs(mock_request_1981185920_36020, tmp_path):
    """test oai loader implementation for opendata"""

    # arrange
    mock_request_1981185920_36020.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = "MAX"
    local_dst = local_dir / (the_id + ".xml")
    request_kwargs = dict(headers={"User-Agent": "Smith"})

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA,
        group_images=key_images,
        post_oai=df_md.extract_mets,
        request_kwargs=request_kwargs,
    )
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


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_opendata_file_identifier(mock_request_1981185920_36020, tmp_path):
    """Ensure OAI Loader switches behavior and renames
    downloaded resources according to FILE@ID rather
    """

    # arrange
    mock_request_1981185920_36020.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir: Path = tmp_path / "WORKDIR" / the_id
    store_dir: Path = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = "MAX"
    dst_path = local_dir / f"{the_id}.xml"

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA,
        group_images=key_images,
        post_oai=df_md.extract_mets,
    )
    loader.store = df_io.LocalStore(store_dir, local_dir)
    number = loader.load(record.identifier, dst_path, use_file_id=True)

    # assert
    assert number == 12
    assert dst_path.is_file()
    assert (local_dir / "MAX" / "FILE_0001_MAX.jpg").is_file()
    assert (local_dir / "MAX" / "FILE_0011_MAX.jpg").is_file()


@unittest.mock.patch("digiflow.requests.get")
def test_oai_load_vls_zd1_with_ocr(mock_request, tmp_path):
    """Behavior of state lists for ocr pipeline"""

    # arrange
    mock_request.side_effect = mock_request_vls_zd1_16359609
    ident = "oai:digitale.bibliothek.uni-halle.de/zd:16359609"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "zd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(base_url=OAI_BASE_URL_ZD, post_oai=df_md.extract_mets)
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


@unittest.mock.patch("requests.get")
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
    record = df_r.Record("foo")
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = "MAX"
    local_dst = str(local_dir) + "/" + the_id + ".xml"
    request_kwargs = dict(headers={"User-Agent": "Smith"})

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA,
        group_images=key_images,
        post_oai=df_md.extract_mets,
        request_kwargs=request_kwargs,
    )
    loader.store = df_io.LocalStore(store_dir, local_dir)

    # act
    with pytest.raises(Exception) as exc:
        loader.load(record.identifier, local_dst, "foo")

    # assert
    assert exc.typename == "ServerError"
    a_msg = exc.value.args[0]
    assert (
        a_msg
        == "https://opendata.uni-halle.de/oai/dd?verb=GetRecord&metadataPrefix=mets&identifier=foo status 504"
    )


@unittest.mock.patch("requests.get")
def test_oai_load_missing_record(mock_requests: unittest.mock.Mock, tmp_path):
    """Fix behavior if record requested which is no longer avaiable
    Example: oai:opendata2.uni-halle.de:1516514412012/175735
    """

    # arrange
    data_path = os.path.join(str(ROOT), "tests/resources/oai/oai-record-missing.xml")
    a_response = mock_response(
        status_code=200,
        headers={"Content-Type": "text/xml;charset=UTF-8"},
        data_path=data_path,
    )
    mock_requests.return_value = a_response
    record = df_r.Record("oai:opendata2.uni-halle.de:1516514412012/175735")
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    store_dir = tmp_path / "STORE" / "dd" / the_id
    local_dir.mkdir(parents=True)
    store_dir.mkdir(parents=True)
    key_images = "MAX"
    local_dst = local_dir / (the_id + ".xml")
    request_kwargs = dict(headers={"User-Agent": "Smith"})

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA,
        group_images=key_images,
        post_oai=df_md.extract_mets,
        request_kwargs=request_kwargs,
    )
    loader.store = df_io.LocalStore(store_dir, local_dir)

    # act
    with pytest.raises(df_io.LoadException) as exc:
        loader.load(record.identifier, local_dst, "foo")

    # assert
    assert exc.typename == "LoadException"
    a_msg = exc.value.args[0]
    assert "The given id does not exist" in str(a_msg)


@unittest.mock.patch("requests.get")
def test_oailoader_with_string_requests_kwargs(
    mock_requests: unittest.mock.Mock, tmp_path
):
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
        df_io.OAI_KWARG_REQUESTS: 'timeout=20, headers={"User-Agent": "ulbbot+IT-WF-OCR-VD17"}'
    }
    loader = df_io.OAILoader(the_url, **raw_kwargs)

    # act
    with pytest.raises(df_io.ContentException) as strange:
        loader.load_resource(the_url, tmp_path, None)

    # assert
    assert "unhandled content-type" in strange.value.args[0]
    assert mock_requests.call_count == 1


@unittest.mock.patch("requests.get")
def test_oai_record_deleted(mock_requests: unittest.mock.Mock, tmp_path):
    """Test handling of deleted record in OAI response"""

    # arrange
    data_path = ROOT / "tests" / "resources" / "oai" / "oai_mets_1981185920_118701.xml"
    a_response = mock_response(
        status_code=200,
        headers={"Content-Type": "text/xml;charset=UTF-8"},
        data_path=data_path,
    )
    mock_requests.return_value = a_response
    the_url = "https://opendata.uni-halle.de/oai/dd"
    loader = df_io.OAILoader(the_url)

    # act
    with pytest.raises(df_io.LoadException) as load_exc:
        loader.load(
            "oai:opendata.uni-halle.de:1981185920/118701", tmp_path / "mets.xml", None
        )

    # assert
    assert "The record has been deleted" in str(load_exc.value.args[0])
    assert mock_requests.call_count == 1
    assert not tmp_path.joinpath("mets.xml").is_file()


# ============================================================================
# Tests for OAILoader.clone method
# ============================================================================


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_mets_only(mock_requests_get, mock_session_get, tmp_path):
    """Test basic clone operation - METS only without resources"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(record.identifier, local_dst, download_resources=False)

    # assert - only METS file should exist
    assert os.path.isfile(str(local_dst))
    assert not os.path.exists(str(local_dir / "MAX"))
    assert not os.path.exists(str(local_dir / "THUMBS"))


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_with_all_resources(mock_requests_get, mock_session_get, tmp_path):
    """Test clone with all resources downloaded"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(record.identifier, local_dst, download_resources=True)

    # assert - METS and all resource directories should exist
    assert os.path.isfile(str(local_dst))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isdir(str(local_dir / "THUMBS"))
    assert os.path.isdir(str(local_dir / "DEFAULT"))
    assert os.path.isdir(str(local_dir / "DOWNLOAD"))
    # check that files were downloaded
    assert len(os.listdir(str(local_dir / "MAX"))) > 0


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_with_specific_type_max_only(
    mock_requests_get, mock_session_get, tmp_path
):
    """Test clone with only MAX fileGroup type"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(record.identifier, local_dst, download_resources=True, types=["MAX"])

    # assert - only MAX resources should be downloaded
    assert os.path.isfile(str(local_dst))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert not os.path.exists(str(local_dir / "THUMBS"))
    assert not os.path.exists(str(local_dir / "DEFAULT"))
    # check that MAX files were downloaded
    max_files = os.listdir(str(local_dir / "MAX"))
    assert len(max_files) == 11  # 11 MAX images in the fixture


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_with_multiple_specific_types(
    mock_requests_get, mock_session_get, tmp_path
):
    """Test clone with multiple specific fileGroup types"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(
        record.identifier, local_dst, download_resources=True, types=["MAX", "DOWNLOAD"]
    )

    # assert - only MAX and DOWNLOAD resources should exist
    assert os.path.isfile(str(local_dst))
    assert os.path.isdir(str(local_dir / "MAX"))
    assert os.path.isdir(str(local_dir / "DOWNLOAD"))
    assert not os.path.exists(str(local_dir / "THUMBS"))
    assert not os.path.exists(str(local_dir / "DEFAULT"))


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_with_progress_callback(
    mock_requests_get, mock_session_get, tmp_path
):
    """Test clone with progress callback tracking"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    local_dst = local_dir / (the_id + ".xml")

    progress_calls = []

    def track_progress(current, total, status_msg):
        progress_calls.append(
            {"current": current, "total": total, "status": status_msg}
        )

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(
        record.identifier,
        local_dst,
        download_resources=True,
        types=["MAX"],
        progress_callback=track_progress,
    )

    # assert - progress callback should have been called
    assert len(progress_calls) > 0
    # should have METS download progress
    assert any("METS" in call["status"] for call in progress_calls)
    # should have resource download progress
    assert any("Downloaded" in call["status"] for call in progress_calls)


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_with_different_n_workers(
    mock_requests_get, mock_session_get, tmp_path
):
    """Test clone with different number of parallel workers"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    local_dst = local_dir / (the_id + ".xml")

    # act - test with single worker
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(
        record.identifier,
        local_dst,
        download_resources=True,
        types=["MAX"],
        n_workers=1,
    )

    # assert - files should be downloaded successfully
    assert os.path.isfile(str(local_dst))
    assert os.path.isdir(str(local_dir / "MAX"))
    max_files = os.listdir(str(local_dir / "MAX"))
    assert len(max_files) == 11


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_with_path_string(mock_requests_get, mock_session_get, tmp_path):
    """Test clone accepts string path as well as Path object"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    local_dir = tmp_path / "WORKDIR" / the_id
    local_dst_str = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(record.identifier, local_dst_str, download_resources=False)

    # assert
    assert os.path.isfile(local_dst_str)


@unittest.mock.patch("digiflow.requests.Session.get")
@unittest.mock.patch("digiflow.requests.get")
def test_oai_clone_creates_directories(mock_requests_get, mock_session_get, tmp_path):
    """Test clone creates necessary directories automatically"""

    # arrange
    mock_requests_get.side_effect = fixture_request_results
    mock_session_get.side_effect = fixture_request_results
    ident = "oai:opendata.uni-halle.de:1981185920/36020"
    record = df_r.Record(ident)
    the_id = record.local_identifier
    # use nested path that doesn't exist yet
    local_dir = tmp_path / "WORKDIR" / "nested" / "path" / the_id
    local_dst = local_dir / (the_id + ".xml")

    # act
    loader = df_io.OAILoader(
        base_url=OAI_BASE_URL_OPENDATA, post_oai=df_md.extract_mets
    )
    loader.clone(record.identifier, local_dst, download_resources=False)

    # assert - directories should be created automatically
    assert os.path.isfile(str(local_dst))
    assert os.path.isdir(str(local_dir))
