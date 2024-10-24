"""Specification for IO API"""

import ast
import json
import json.decoder
import os
import shutil
import sys
import unittest.mock

from pathlib import Path


import pytest

import digiflow.record as df_r

from digiflow import (
    LoadException,
    get_enclosed,
    request_resource,
)

from .conftest import TEST_RES, LEGACY_HEADER_STR, write_datalist

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
                         [('oai:digital.bibliothek.uni-halle.de/hd:10595', '10595'),
                          ('oai:digitale.bibliothek.uni-halle.de/vd18:9427342', '9427342'),
                          ('oai:opendata.uni-halle.de:1981185920/34265', '1981185920_34265'),
                          ('oai:dev.opendata.uni-halle.de:123456789/27949', '123456789_27949')])
def test_record_local_identifiers(urn, local_identifier):
    """Ensure local identifier for different URN inputs"""

    # act
    record = df_r.Record(urn)
    assert record.local_identifier == local_identifier
    assert urn in str(record)


def test_invalid_input_data(tmp_path):
    """Invalid input data format raises an exception"""

    # arrange
    invalid_path_dir = tmp_path / 'invalid_data'
    invalid_path_dir.mkdir()
    invalid_path = invalid_path_dir / 'invalid.tsv'
    data = ["123\t456\t789\t0\n", "124\t457\t790\t1\n"]
    write_datalist(invalid_path, data, headers=None)

    with pytest.raises(df_r.RecordHandlerException) as exc:
        df_r.RecordHandler(invalid_path, data_fields=[df_r.FIELD_IDENTIFIER, df_r.FIELD_STATE])

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
    write_datalist(valid_path, data)
    return str(valid_path)


def test_migrate_state_saved(valid_datasets):
    """
    Valid input data format enables to mark
    successfull outcomes by row nr.5 & nr. 6
    """

    # arrange
    handler = df_r.RecordHandler(valid_datasets, mark_lock='ocr_done')
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
    write_datalist(valid_path, data)
    return str(valid_path)


def test_migration_dataset_vl_formats(vl_datasets):
    """
    Valid input data format enables to mark
    successfull outcomes by row nr.5 & nr. 6
    """

    # arrange
    handler = df_r.RecordHandler(vl_datasets)
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
    handler = df_r.RecordHandler(vl_datasets)
    a_set = handler.next_record()

    # assert
    assert a_set.local_identifier == '1416976'
    handler.save_record_state(a_set.identifier, 'metadata_done', INFO='123,ger')
    handler.save_record_state(a_set.identifier, 'migration_done')

    a_set = handler.next_record(state='migration_done')
    assert a_set.local_identifier == '1416976'
    with open(vl_datasets, encoding='utf8') as reader:
        lines = reader.readlines()
    assert lines[1].split('\t')[3] == '123,ger'


def test_dataset_cannot_find_entry(vl_datasets):
    """Behavior if want to store with unknown identifier"""

    # arrange
    handler = df_r.RecordHandler(vl_datasets)

    # act
    with pytest.raises(RuntimeError) as exc:
        handler.save_record_state('foo')

    # assert
    assert 'No Record for foo' in str(exc.value)


def test_statelist_ocr_hdz(tmp_path):
    """Behavior of state lists for ocr pipeline"""

    # arrange
    path_state_list = tmp_path / 'ocr_list'
    first_row = f"{df_r.FIELD_IDENTIFIER}\t{df_r.FIELD_STATE}\t{df_r.FIELD_STATETIME}\n"
    data = [
        'oai:digitale.bibliothek.uni-halle.de/zd:123\tn.a.\tn.a.\n'
    ]
    write_datalist(path_state_list, data, first_row)
    _headers = [df_r.FIELD_IDENTIFIER, df_r.FIELD_STATE, df_r.FIELD_STATETIME]
    handler = df_r.RecordHandler(
        path_state_list,
        data_fields=_headers,
        transform_func=lambda r: r)

    # act
    record = handler.next_record()
    assert handler.position == '0001/0001'
    assert record[df_r.FIELD_IDENTIFIER] == 'oai:digitale.bibliothek.uni-halle.de/zd:123'
    handler.save_record_state(record[df_r.FIELD_IDENTIFIER])

    # assert no next open record
    assert not handler.next_record()

    # assert there is a record next with default state 'lock'
    assert handler.next_record('busy')


def test_statelist_use_n_a_properly(oai_record_list):
    """Behavior of state lists for ocr pipeline"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)

    # act
    record = handler.next_record()
    assert handler.position == '0006/0006'
    assert record.identifier == 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    handler.save_record_state(record.identifier)

    # assert no next open record
    assert not handler.next_record()

    # assert there is a record with default state 'lock'
    assert handler.next_record('busy')


def test_record_datestamp(oai_record_list):
    """Check if proper datestamp gets picked"""

    # arrange
    hndlr = df_r.RecordHandler(oai_record_list)

    # act
    rcrd: df_r.Record = hndlr.next_record()

    # assert
    assert rcrd.identifier == 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    assert rcrd.local_identifier == '9510508'
    assert rcrd.created_time == '2015-08-25T20:00:35Z'


def test_record_get_fullident(oai_record_list):
    """Check if proper datestamp gets picked"""

    # arrange
    ident_urn = 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    hndlr = df_r.RecordHandler(oai_record_list)

    # act
    rcrd: df_r.Record = hndlr.get(ident_urn)

    # assert
    assert rcrd.identifier == ident_urn
    assert rcrd.created_time == '2015-08-25T20:00:35Z'


def test_record_get_partialident(oai_record_list):
    """Check if proper datestamp gets picked
    if just the latest segment (aka local
    identifier) has been provided"""

    # arrange
    ident_urn = 'oai:digitale.bibliothek.uni-halle.de/zd:9510508'
    handler = df_r.RecordHandler(oai_record_list)

    # act
    record_exact = handler.get('9510508')
    record_fuzzy: df_r.Record = handler.get('9510508', exact_match=False)

    # assert
    assert not record_exact
    assert record_fuzzy.identifier == ident_urn
    assert record_fuzzy.created_time == '2015-08-25T20:00:35Z'


def test_record_get_non_existent(oai_record_list):
    """What happens if no match found?"""

    # arrange
    _handler = df_r.RecordHandler(oai_record_list)

    # act
    assert not _handler.get('9510509')


def test_record_handler_merge_larger_into_smaller_dry_run(tmp_path):
    """Behavior if larger list gets merged into smaller.
    Question: How to deal with unknown dataset?
    """

    # arrange
    path_oai_list_a = tmp_path / 'oai_list_a'
    data = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n"
    ]
    write_datalist(path_oai_list_a, data, LEGACY_HEADER_STR)
    handler = df_r.RecordHandler(
        path_oai_list_a,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    path_oai_list_b = tmp_path / 'oai_list_b'
    data = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    write_datalist(path_oai_list_b, data, LEGACY_HEADER_STR)

    # assert original next == first record with identifier 8853012
    assert handler.next_record(state='upload_done').identifier.endswith('8853012')

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
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n"
    ]
    write_datalist(path_oai_list_a, data1, LEGACY_HEADER_STR)
    handler = df_r.RecordHandler(
        path_oai_list_a,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    path_oai_list_b = tmp_path / 'oai_list_b'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
    ]
    write_datalist(path_oai_list_b, data2, LEGACY_HEADER_STR)

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
    assert handler.next_record(state='upload_done').identifier.endswith('8853012')
    # ... and this shall be second record by now
    assert handler.next_record().identifier.endswith('8853013')


def test_record_handler_merge_larger_into_smaller_hot_run_inverse(tmp_path):
    """Ensure following Error is gone:
    'UnboundLocalError: local variable 'self_record' referenced before assignment'

    Yielded because record 1 from list 2 is unknown.
    """

    # arrange
    path_oai_list_a = tmp_path / 'oai_list_a'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n"
    ]
    write_datalist(path_oai_list_a, data1, LEGACY_HEADER_STR)
    handler = df_r.RecordHandler(
        path_oai_list_a,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    path_oai_list_b = tmp_path / 'oai_list_b'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n"
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
    ]
    write_datalist(path_oai_list_b, data2, LEGACY_HEADER_STR)

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
    assert record_012.identifier.endswith('8853012')
    # mark state record 1
    handler.save_record_state(record_012.identifier, state='foo_bar')
    # ... this shall be second record by now
    assert handler.next_record().identifier.endswith('8853013')


def test_record_handler_merge_cross(tmp_path):
    """
    Behavior merging 2 lists with cross-different
    info, with info from list 1 must be preserved
    but new info from list 2 must be integrated
    """

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
    ]
    write_datalist(path_oai_list1, data1, LEGACY_HEADER_STR)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\t2015-08-25T20:00:35Z\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\t2015-08-25T20:00:35Z\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    write_datalist(path_oai_list2, data2, LEGACY_HEADER_STR)
    handler = df_r.RecordHandler(
        path_oai_list1,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)

    # assert original next == first record with identifier 8853012
    assert handler.next_record().identifier.endswith('8853013')

    # act
    results = handler.merges(path_oai_list2, dry_run=False)

    # act
    assert results['merges'] == 1  # because info got merged from 2 record
    assert results['ignores'] == 1  # nothing missed, because all idents present
    assert not results['appendeds']  # no new, because all idents present
    assert not handler.next_record(state='n.a.')    # by now no more open records


def test_records_default_header_from_file(oai_record_list):
    """Ensure proper range has been selected"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        transform_func=df_r.row_to_record)

    # act
    new_path = handler.frame(3)

    # assert
    assert os.path.exists(new_path)
    assert os.path.basename(new_path) == 'ocr_list_03_06.csv'

    frame_handler = df_r.RecordHandler(
        new_path,
        transform_func=df_r.row_to_record)

    # ensure that by now 4 records are set to 'other_load'
    # first + second record only
    c_state = df_r.State(df_r.RECORD_STATE_MASK_FRAME)
    assert frame_handler.states([c_state]) == 2


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


def test_records_sample_zd1_post_ocr():
    """Ensure proper state recognized"""

    path_list = os.path.join(str(ROOT), 'tests', 'resources',
                             'vls', 'oai-urn-zd1-sample70k.tsv')
    assert os.path.isfile(path_list)

    # arrange
    handler = df_r.RecordHandler(
        path_list)
    crit1 = df_r.Datetime(dt_from='2021-10-16_09:45:00')

    # assert
    assert 1 == handler.states([crit1])
    crit2 = df_r.State('other_load')
    assert 10 == handler.states([crit2])


def test_recordcriteria_with_created_datetime_format():
    """Expect 12 records to have a CREATED datetime before 15:26:00"""

    path_list = os.path.join(str(ROOT), 'tests', 'resources',
                             'vls', 'oai-urn-zd1-sample70k.tsv')
    assert os.path.isfile(path_list)

    # arrange
    handler = df_r.RecordHandler(
        path_list)
    crit1 = df_r.Datetime(
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
    write_datalist(path_oai_list1, the_data)
    handler = df_r.RecordHandler(
        path_oai_list1,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    crit1 = df_r.Text('no colorchecker')
    crit2 = df_r.Text('no_publ_place')

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
    handler = df_r.RecordHandler(path_oai_list1, transform_func=df_r.row_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    with pytest.raises(json.decoder.JSONDecodeError) as _decode_err:
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
    handler = df_r.RecordHandler(path_oai_list1, transform_func=df_r.row_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    with pytest.raises(json.decoder.JSONDecodeError) as _decode_err:
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
    handler = df_r.RecordHandler(path_oai_list1, transform_func=df_r.row_to_record)

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
    handler = df_r.RecordHandler(path_oai_list1, transform_func=df_r.row_to_record)

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
    handler = df_r.RecordHandler(path_oai_list1, transform_func=df_r.row_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_fail')
    assert _next_rec.info.startswith('141.48.10.202@2023-01-17_15:55:49')
    _info_token = get_enclosed(_next_rec.info)
    _last_info = ast.literal_eval(_info_token)['info']
    assert "processing 'https://opendata.uni-halle.de/retrieve/b7f7f81d-e65f-4c7d-95c6-7384b184c6a9/00001051.jpg'" in _last_info


@pytest.mark.skipif(sys.version_info >= (3, 10), reason="Specific to Python <3.10")
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
    handler = df_r.RecordHandler(path_oai_list1, transform_func=df_r.row_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_busy')
    _info_token = get_enclosed(_next_rec.info)
    with pytest.raises(SyntaxError) as _decode_err:
        ast.literal_eval(_info_token)
    assert 'invalid syntax' in _decode_err.value.args[0]


@pytest.mark.skipif(sys.version_info < (3, 10),
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
    handler = df_r.RecordHandler(path_oai_list1, transform_func=df_r.row_to_record)

    # act
    _next_rec = handler.next_record(state='ocr_busy')
    _info_token = get_enclosed(_next_rec.info)
    with pytest.raises(SyntaxError) as _decode_err:
        ast.literal_eval(_info_token)
    assert "unmatched '}'" in _decode_err.value.args[0]


def test_record_handler_with_broken_row(tmp_path):
    """
    Behavior when encountering broken rows
    Pevent KeyError: 'STATE'
    """

    # arrange
    path_list_sample = os.path.join(TEST_RES, 'zkw_vd18_phase4_01.csv')
    path_oai_list1 = tmp_path / 'zkw.csv'
    tmp_res_path = shutil.copy(path_list_sample, path_oai_list1)
    with open(tmp_res_path, 'a', encoding='utf-8') as tmp_file:
        tmp_file.write('\n')

    handler = df_r.RecordHandler(tmp_res_path,
                                 transform_func=df_r.row_to_record)

    # act
    with pytest.raises(df_r.RecordHandlerException) as handl_exc:
        handler.next_record(state='foo')  # dummy state to provoke error

    assert "line:003 no STATE field " in handl_exc.value.args[0]


def test_record_handler_merge_info_dicts(tmp_path):
    """Two lists merged into one and result
    info field was merged too since it's a literal 
    python dict.

    Please note:
    This merge will only work if both INFO fields
    can evaluate to dictionaries!
    """

    # arrange
    path_oai_list_a = tmp_path / 'oai_list_a'
    data_fresh = [
        "123\tn.a.\t2015-08-25T20:00:35Z\t{'pages':23, 'ods_created':'1984-10-03'}\tu.a.\tn.a.\n"
        "124\tn.a.\t2015-08-25T20:00:35Z\t{'pages':24, 'ods_created':'1985-05-05'}\tn.a.\tn.a.\n"
    ]
    write_datalist(path_oai_list_a, data_fresh, LEGACY_HEADER_STR)
    dst_hndlr = df_r.RecordHandler(
        path_oai_list_a,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)

    list_merge = tmp_path / 'oai_list_b'
    data2 = [
        "123\tn.a.\t2015-08-25T20:00:35Z\t{'pages':23, 'n_ocr':20}\tocr_done\t2024-10-18_11:12:00\n",
    ]
    write_datalist(list_merge, data2, LEGACY_HEADER_STR)

    # act: must *not* set ignore_state=None,
    # otherwise first record from second list erases
    # existing data from list a
    results = dst_hndlr.merges(list_merge, dry_run=False)
    merged_record: df_r.Record = dst_hndlr.next_record(state='ocr_done')

    # no merge since state 'n.a.' from list b
    # first record of list b ignored
    # to preserve existing data from list a
    assert results['merges'] == 1
    assert results['ignores'] == 0
    assert results['appendeds'] == 0
    assert dst_hndlr.total_len == 2
    assert merged_record.info == {'n_ocr': 20, 'pages': 23, 'ods_created': '1984-10-03'}


def test_record_handler_merge_write_read(tmp_path):
    """Two lists info field merged too but managed
    to handle quotations around the info-string.
    """

    # arrange
    path_oai_list_a = tmp_path / 'oai_list_a'
    data_fresh = [
        "123\tn.a.\t2015-08-25T20:00:35Z\t{'pages':23, 'ods_created':'1984-10-03'}\tu.a.\tn.a.\n"
    ]
    write_datalist(path_oai_list_a, data_fresh, LEGACY_HEADER_STR)
    dst_hndlr = df_r.RecordHandler(
        path_oai_list_a,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)

    list_merge = tmp_path / 'oai_list_b'
    data2 = [
        "123\tn.a.\t2015-08-25T20:00:35Z\t\"{'xml_invalid': \"Element 'mods:subtitle': This element is not expected.\"}\"\tocr_done\t2024-10-18_11:12:00\n",
    ]
    write_datalist(list_merge, data2, LEGACY_HEADER_STR)

    # act
    dst_hndlr.merges(list_merge, dry_run=False)
    new_hndlr = df_r.RecordHandler(path_oai_list_a,
                                   data_fields=df_r.LEGACY_HEADER,
                                   transform_func=df_r.row_to_record)

    # assert
    tha_record: df_r.Record = new_hndlr.next_record(state='ocr_done')
    assert tha_record.info == {'pages': 23,
                               'ods_created': '1984-10-03',
                               'xml_invalid': "Element 'mods:subtitle': This element is not expected."}
