"""Specification for IO API"""

import os
from pathlib import Path

import digiflow.record.record_criteria as df_rcr

import digiflow.record as df_r

from .conftest import RECORD_HEADER_STR, write_datalist

ROOT = Path(__file__).parents[1]

EXPORT_METS = 'export_mets.xml'


def test_record_state_list_set_state_from(oai_record_list):
    """Behavior for changing state matching state and start time"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    c_state = df_rcr.State('ocr_skip')
    c_from = df_rcr.Datetime(dt_from='2021-08-03_15:03:56')

    # pre-check
    record1: df_r.Record = handler.next_record()
    assert record1.identifier.endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_from], dry_run=False)

    # assert next open record changed
    record2: df_r.Record = handler.next_record()
    assert record2.identifier.endswith('8853011')
    # assert we handled 3 records
    assert outcome == 3


def test_record_state_list_set_state_from_dry_run(oai_record_list):
    """Behavior of dry run"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    c_state = df_rcr.State('ocr_skip')
    c_from = df_rcr.Datetime(dt_from='2021-08-03_15:03:56')

    # pre-check
    record1: df_r.Record = handler.next_record()
    assert record1.identifier.endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_from], dry_run=True)

    # assert no change of date
    record2: df_r.Record = handler.next_record()
    assert record2.identifier.endswith('9510508')
    assert outcome == 3


def test_record_state_list_set_state_from_dry_run_verbose(oai_record_list, capsys):
    """Behavior of dry run"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    c_state = df_rcr.State('ocr_skip')
    c_from = df_rcr.Datetime(dt_from='2021-08-03_15:03:56')

    # pre-check
    record1 = handler.next_record()
    assert record1.identifier.endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_from], dry_run=True, verbose=True)

    # assert
    assert outcome == 3
    _std_out = capsys.readouterr().out
    assert 'IDENTIFIER\tSETSPEC\tCREATED\tINFO\tSTATE\tSTATE_TIME' in _std_out
    assert 'oai:digitale.bibliothek.uni-halle.de/zd:8853012' in _std_out


def test_record_state_list_rewind_state_upload(oai_record_list):
    """Behavior for changing state"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    c_state = df_rcr.State('upload_done')

    # pre-check
    record1 = handler.next_record()
    assert record1.identifier.endswith('9510508')

    # act
    handler.states(criterias=[c_state], dry_run=False)
    assert handler.next_record().identifier.endswith('17320046')

    # assert next open record changed
    h2 = df_r.RecordHandler(
        oai_record_list, data_fields=df_r.LEGACY_HEADER, transform_func=df_r.row_to_record)
    assert h2.next_record().identifier.endswith('17320046')


def test_record_set_some_other_state(oai_record_list):
    """Behavior for changing state"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    c_state = df_rcr.State('upload_done')

    # pre-check
    record1 = handler.next_record()
    assert record1.identifier.endswith('9510508')

    # act
    handler.states(
        criterias=[c_state], set_state='metadata_read', dry_run=False)
    assert not handler.next_record(state='upload_done')

    # assert next open record changed
    h2 = df_r.RecordHandler(
        oai_record_list, data_fields=df_r.LEGACY_HEADER, transform_func=df_r.row_to_record)
    assert h2.next_record(state='metadata_read').identifier.endswith('17320046')


def test_record_list_time_range(oai_record_list):
    """Behavior for setting state within time range"""

    # arrange
    handler = df_r.RecordHandler(
        oai_record_list,
        data_fields=df_r.LEGACY_HEADER,
        transform_func=df_r.row_to_record)
    c_state = df_rcr.State('ocr_skip')
    c_range = df_rcr.Datetime(dt_from='2021-08-03_15:10:00',
                              dt_to='2021-08-03_15:20:00')

    # pre-check
    record1 = handler.next_record()
    assert record1.identifier.endswith('9510508')

    # act
    outcome = handler.states(criterias=[c_state, c_range], dry_run=False)
    assert outcome == 1

    # assert next open record changed
    record2 = handler.next_record()
    assert record2.identifier.endswith('8853012')


def test_record_handler_merge_plain(tmp_path):
    """Behavior if merging 2 lists"""

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:uni-halle.de/zd:8853012\tn.a.\tn.a.\t2015-08-25T20:00:35Z\n",
        "oai:uni-halle.de/zd:8853013\tn.a.\tn.a.\t2015-08-25T20:00:35Z\n",
        "oai:uni-halle.de/zd:9510507\tn.a.\tn.a.\t2015-08-25T20:00:35Z\n"
    ]
    write_datalist(path_oai_list1, data1, RECORD_HEADER_STR)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:uni-halle.de/zd:8853012\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:uni-halle.de/zd:9510507\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    write_datalist(path_oai_list2, data2, RECORD_HEADER_STR)
    handler = df_r.RecordHandler(
        path_oai_list1,
        data_fields=df_r.RECORD_HEADER,
        transform_func=df_r.row_to_record)

    # assert original next == first record with identifier 8853012
    assert handler.next_record().identifier.endswith('8853012')

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
    assert handler.next_record().identifier.endswith('8853013')
    # state from record with ident '8853012' is now 'upload_done'
    criterias = [df_rcr.Identifier(
        '8853012'), df_rcr.State('upload_done')]
    assert handler.states(criterias=criterias) == 1


def test_record_handler_merge_only_done(tmp_path):
    """Behavior if merging 2 lists with specific requirement"""

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\tn.a.\tn.a.\tn.a.\n"
    ]
    write_datalist(path_oai_list1, data1, RECORD_HEADER_STR)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    write_datalist(path_oai_list2, data2, RECORD_HEADER_STR)
    handler = df_r.RecordHandler(
        path_oai_list1,
        data_fields=df_r.RECORD_HEADER,
        transform_func=df_r.row_to_record)

    # act
    merge_result = handler.merges(path_oai_list2, dry_run=False, other_require_state='upload_done')

    # list1 and list2 share 2 records
    assert merge_result['matches'] == 2
    # list2 didn't contain any records *not* included in list1
    assert merge_result['misses'] == 0
    # only the required record was merged
    assert merge_result['merges'] == 1
    # no new records
    assert merge_result['appendeds'] == 0
    # next must have changed, since 'n.a.' was merged with 'upload_done'
    assert handler.next_record().identifier.endswith('8853013')
    # state from record with ident '8853012' is now 'upload_done'
    criterias = [df_rcr.Identifier(
        '8853012'), df_rcr.State('upload_done')]
    assert handler.states(criterias=criterias) == 1


def test_record_handler_merge_ignore_failure(tmp_path):
    """Behavior if merging 2 lists but ignore failures"""

    # arrange
    path_oai_list1 = tmp_path / 'oai_list1'
    data1 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:8853013\tn.a.\tn.a.\tn.a.\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\tn.a.\tn.a.\tn.a.\n"
    ]
    write_datalist(path_oai_list1, data1, RECORD_HEADER_STR)
    path_oai_list2 = tmp_path / 'oai_list2'
    data2 = [
        "oai:digitale.bibliothek.uni-halle.de/zd:8853012\tinfo1\tupload_done\t2021-08-03_15:14:45\n",
        "oai:digitale.bibliothek.uni-halle.de/zd:9510507\tinfo2\tocr_fail\t2021-08-03_16:14:45\n"
    ]
    write_datalist(path_oai_list2, data2, RECORD_HEADER_STR)
    handler = df_r.RecordHandler(
        path_oai_list1,
        data_fields=df_r.RECORD_HEADER,
        transform_func=df_r.row_to_record)

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
    assert handler.next_record().identifier.endswith('8853012')
    # state from record with ident '9510507' is now 'ocr_fail'
    criterias = [df_rcr.Identifier(
        '9510507'), df_rcr.State('ocr_fail')]
    assert handler.states(criterias=criterias) == 1


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
    c_state = df_rcr.State(df_r.RECORD_STATE_MASK_FRAME)
    assert frame_handler.states([c_state]) == 2


def test_records_sample_zd1_post_ocr():
    """Ensure proper state recognized"""

    path_list = os.path.join(str(ROOT), 'tests', 'resources',
                             'vls', 'oai-urn-zd1-sample70k.tsv')
    assert os.path.isfile(path_list)

    # arrange
    handler = df_r.RecordHandler(path_list)
    crit1 = df_rcr.Datetime(dt_from='2021-10-16_09:45:00')

    # assert
    assert 1 == handler.states([crit1])
    crit2 = df_rcr.State('other_load')
    assert 10 == handler.states([crit2])


def test_recordcriteria_with_created_datetime_format():
    """Expect 12 records to have a CREATED datetime before 15:26:00"""

    path_list = os.path.join(str(ROOT), 'tests', 'resources',
                             'vls', 'oai-urn-zd1-sample70k.tsv')
    assert os.path.isfile(path_list)

    # arrange
    handler = df_r.RecordHandler(
        path_list)
    crit1 = df_rcr.Datetime(
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
    crit1 = df_rcr.Text('no colorchecker')
    crit2 = df_rcr.Text('no_publ_place')

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
