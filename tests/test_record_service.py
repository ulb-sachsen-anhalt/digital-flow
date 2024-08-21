"""API record service"""

import ast
import typing

import pytest

import digiflow.record as df_r


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
