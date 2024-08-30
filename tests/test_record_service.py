"""API record service"""

import ast
import typing
import unittest.mock

import requests.exceptions as req_ex
import pytest

import digiflow.record as df_r
import digiflow.record.record_service as df_rs


RECORD_IDENTIFIER = 'IDENTIFIER'
RECORD_INFO = 'INFO'
INFO_N_OCR = 'n_ocr'


def test_update_record_info_plain():
    """Behavior when updating record 
    with single, common info field
    """

    # arrange
    the_urn = "oai:opendata.uni-halle.de:1981185920/38841"
    rec: typing.Dict = {RECORD_IDENTIFIER: the_urn}
    rec[RECORD_INFO] = "{'gvk-ppn': '1764064194', 'pica': 'Aa', 'pages': 9, 'languages': ['ger']}"
    new_kwargs = {INFO_N_OCR: 5}

    # act
    curr_info = ast.literal_eval(rec[RECORD_INFO])
    curr_info.update(**new_kwargs)
    rec[RECORD_INFO] = curr_info

    # assert
    assert isinstance(curr_info, dict)
    assert INFO_N_OCR in rec[RECORD_INFO]
    assert rec[RECORD_INFO][INFO_N_OCR] == 5


def test_explore_update_record_info_with_multiple_info_entries():
    """Behavior when updating record 
    with single, common info field
    """

    # arrange
    the_urn = "oai:opendata.uni-halle.de:1981185920/38841"
    rec: typing.Dict = {RECORD_IDENTIFIER: the_urn}
    rec[RECORD_INFO] = "{'ppn': '334587093', 'pica': 'Af', 'pages': 575, 'languages': ['ger']},\
        				{'client': '141.48.10.246'},\
                        {'ppn': '334587093', 'pica': 'Af', 'pages': 575, 'languages': ['ger']}"
    new_kwargs = {INFO_N_OCR: 5}

    # act
    curr_info = ast.literal_eval(rec[RECORD_INFO])

    with pytest.raises(AttributeError) as attr_exc:
        curr_info.update(**new_kwargs)

    # assert
    assert isinstance(curr_info, tuple)
    assert "'tuple' object has no attribute 'update'" in attr_exc.value.args[0]


def test_fix_update_record_info_with_multiple_info_entries():
    """Behavior when updating record 
    with single, common info field
    """

    # arrange
    the_urn = "oai:opendata.uni-halle.de:1981185920/38841"
    rec: typing.Dict = {RECORD_IDENTIFIER: the_urn}
    rec[RECORD_INFO] = "{'ppn': '334587093', 'pica': 'Af', 'pages': 575, 'languages': ['ger']},\
        				{'client': '141.48.10.246'},\
                        {'ppn': '334587093', 'pica': 'Af', 'pages': 575, 'languages': ['ger']}"
    new_kwargs = {INFO_N_OCR: 5}

    # act
    curr_info = ast.literal_eval(rec[RECORD_INFO])
    if isinstance(curr_info, tuple):
        curr_info[-1].update(**new_kwargs)
        rec[RECORD_INFO] = f'{curr_info[-1]}'

    # assert
    assert isinstance(rec, dict)
    assert f"'{INFO_N_OCR}': 5" in rec[RECORD_INFO]
    expected = "{'ppn': '334587093', 'pica': 'Af', 'pages': 575, 'languages': ['ger'], 'n_ocr': 5}"
    assert expected == rec[RECORD_INFO]


def test_record_update_dict_and_string():
    """Common situation, when existing string needs
    to be merged with dictionary to prevent

    TypeError: Record.info() takes 1 positional argument but 2 were given
    """

    # arrange
    the_urn = "oai:opendata.uni-halle.de:1981185920/38841"
    org_info = "{'ppn': '334587093', 'pica': 'Af', 'pages': 575, 'languages': ['ger']}"
    record: df_r.Record = df_r.Record(the_urn)
    record.info = "{'ppn': '334587093', 'pica': 'Af', 'pages': 575, 'languages': ['ger']}"
    to_update = {'client': '127.0.0.1'}
    record.info = org_info

    # act
    record.info = to_update

    # assert
    assert 'ppn' in record.info
    assert 'client' in record.info


def test_record_update_dealing_invalid_data():
    """Common situation with legacy INFO data
    ValueError: malformed node or string
    """

    # arrange
    the_urn = "oai:opendata.uni-halle.de:1981185920/38841"
    org_info = "ppn#334587093, pica#Af', 'pages': 575, 'languages': ['ger']"
    record: df_r.Record = df_r.Record(the_urn)

    # act
    record.info = org_info

    # assert
    assert 'ppn#3345' in record.info


@pytest.mark.parametrize("state,file_path,result",
                         [
                             ('n.a.', '/data/oai/test.csv', 'no records n.a. in /data/oai/test.csv'),
                             ('n.a.', '', 'no records n.a. in '),
                             (None, None, 'no records None in None')
                         ])
def test_mark_exhausted_matching(state, file_path, result):
    """Check formatting behavior"""

    # assert
    assert df_rs.DATA_EXHAUSTED_MARK.format(state, file_path) == result


@unittest.mock.patch('digiflow.requests.get')
def test_exit_on_data_exhausted(mock_request):
    """Ensure dedicated state is communicated 
    to OAIClient when no more records present

    Please note: *real* responses return
    byte-object rather!
    """

    # arrange
    record_state = 'ocr_busy'
    list_label = 'oai-record-test'
    srv_rsp = f'{df_rs.DATA_EXHAUSTED_MARK.format(record_state, list_label)}'.encode()
    client = df_r.Client(list_label, '1.2.3.4', '9999')
    mock_resp = unittest.mock.Mock()
    mock_resp.status_code = 404
    mock_resp.headers = {'Content-Type': 'text/xml'}
    mock_resp.content = srv_rsp
    mock_request.return_value = mock_resp

    # act
    with pytest.raises(df_r.RecordsExhaustedException) as recs_ex:
        client.get_record(get_record_state=df_r.UNSET_LABEL, set_record_state='busy')

    # assert
    assert recs_ex.value.args[0] == f'no records {record_state} in {list_label}'


@unittest.mock.patch('digiflow.requests.get')
def test_client_connection_timeout(mock_request):
    """Behavior when server down"""

    # arrange
    a_label = 'oai-record-test'
    client = df_r.Client(a_label, '1.2.3.4', '9999')
    exc_msg = f'connection timeout after {client.timeout_secs} seconds'
    mock_request.side_effect = req_ex.ConnectTimeout(exc_msg)

    # act
    with pytest.raises(df_r.RecordsServiceException) as rse:
        client.get_record(get_record_state=df_r.UNSET_LABEL, set_record_state='busy')

    # assert
    assert exc_msg in rse.value.args[0]
