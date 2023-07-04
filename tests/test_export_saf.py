"""Test [S]imple[A]rchive[F]ile generation format required for DSpace delivery"""

import os
import shutil
from unittest.mock import DEFAULT

import pytest

from digiflow import (
    export_data_from,
    map_contents,
    ExportMapping,
    DEFAULT_EXPORT_MAPPINGS,
    DigiFlowExportError,
    BUNDLE_PREVIEW as BP,
    BUNDLE_THUMBNAIL as BT,
)

from .conftest import (
    TEST_RES
)

SHARE_IT_EXPORT_COLLECTION = '123456789/20952'
THE_METS = 'mets.xml'
KITODO2_MONOGRAPHY_METS = '319696111.xml'
FAKE_METS = '12345678.xml'
FAKE_PDF = '12345678.pdf'


@pytest.fixture(name="monography_319696111")
def fixture_monography_319696111(tmp_path):
    """fixture for 319696111_staging_BooksizeS"""

    src_dir = os.path.join(TEST_RES, '319696111_mono')
    dst_dir = tmp_path / 'content'
    shutil.copytree(src_dir, dst_dir)
    work_dir = tmp_path / "export_workdir"
    work_dir.mkdir()
    exp_dir = tmp_path / "export_finished"
    exp_dir.mkdir()
    yield (dst_dir, work_dir, exp_dir)


def test_export_mappings_mets(monography_319696111):
    """Behavior with default paths for matchings
    between source directory (kitodo: contents)
    and working directory"""

    # arrange
    (cnt_dir, work_dir, _) = monography_319696111

    # act
    export_mappings = map_contents(cnt_dir, work_dir)

    # assert
    assert len(export_mappings) == 1
    assert export_mappings[0].path_source.endswith(KITODO2_MONOGRAPHY_METS)
    assert export_mappings[0].path_target.endswith(THE_METS)


def test_export_mappings_invalid_workdir(monography_319696111):
    """Behavior with default matching paths
    TODO clear concerns"""

    # arrange
    (w_dir, _ ,_ ) = monography_319696111

    # act
    with pytest.raises(DigiFlowExportError) as export_err:
        ExportMapping(str(w_dir)[5:], w_dir.parent)

    # assert
    assert "Invalid source path" in str(export_err.value)


EXPORT_MAPPING_KITODO2 = {'.xml': THE_METS, '.pdf': None, 'jpgs/max': None}
def test_export_kitodo2_mappings(monography_319696111):
    """Behavior with default mappings which
    correspond valid data"""

    # arrange
    (src_dir, work_dir, _) = monography_319696111

    # act
    content_files = map_contents(src_dir, work_dir, EXPORT_MAPPING_KITODO2)

    # assert
    assert len(content_files) == 69
    for content_file in content_files:
        last_src_segm = content_file.path_source.split('/')[-1]
        last_dst_segm = content_file.path_target.split('/')[-1]
        if last_dst_segm == 'mets.xml':
            continue
        assert last_src_segm == last_dst_segm


def test_export_kitodo2_result(monography_319696111):
    """
    create export with monography data from kitodo2
    """

    # arrange
    (cnt_dir, _, exp_dir) = monography_319696111
    process_metafile = os.path.join(cnt_dir, KITODO2_MONOGRAPHY_METS)

    # act
    result = export_data_from(
        process_metafile, SHARE_IT_EXPORT_COLLECTION,
        export_map=EXPORT_MAPPING_KITODO2,
        saf_final_name='319696111', export_dst=exp_dir)

    # assert
    final_location = str(os.path.join(exp_dir, '319696111.zip'))
    assert len(result) == 2
    # changed due intermediate ".processing" - suffix
    assert result[0].startswith(final_location)
    assert result[1] == '0MB'


def test_export_kitodo2_inspect_saf_assets(monography_319696111):
    """
    create export with monography data from kitodo2
    """

    # arrange
    (cnt_dir, _, exp_dir) = monography_319696111
    process_metafile = os.path.join(cnt_dir, KITODO2_MONOGRAPHY_METS)

    # act
    export_data_from(
        process_metafile, SHARE_IT_EXPORT_COLLECTION,
        export_map=EXPORT_MAPPING_KITODO2,
        saf_final_name='319696111', export_dst=exp_dir)

    # respect intermediate ".processing" - suffix
    # required for calling shutil - otherwise claims 
    # ".processing" is invalid archive format
    _tmp_saf_file_name = next(filter(lambda e: 'zip' in e, os.listdir(exp_dir)))
    _final_saf_file_name = _tmp_saf_file_name.replace('.processing', '')
    os.rename(os.path.join(exp_dir, _tmp_saf_file_name), 
              os.path.join(exp_dir, _final_saf_file_name))
    final_saf_path = str(os.path.join(exp_dir, _final_saf_file_name))
    shutil.unpack_archive(final_saf_path, extract_dir=exp_dir)

    # assert
    assert os.path.exists(os.path.join(exp_dir, 'item_000'))
    assert os.path.exists(os.path.join(exp_dir, 'item_000', '00000001.jpg'))
    assert os.path.exists(os.path.join(exp_dir, 'item_000', '319696111.pdf'))


def test_export_kitodo2_inspect_saf_contents_file(monography_319696111):
    """Inspect generated contents file"""

    # arrange
    (cnt_dir, _, exp_dir) = monography_319696111
    process_metafile = os.path.join(cnt_dir, KITODO2_MONOGRAPHY_METS)

    # act
    export_data_from(
        process_metafile, SHARE_IT_EXPORT_COLLECTION,
        export_map=EXPORT_MAPPING_KITODO2,
        saf_final_name='319696111', export_dst=exp_dir)
    
    # respect intermediate ".processing" - suffix
    # required for calling shutil - otherwise claims 
    # ".processing" is invalid archive format
    _tmp_saf_file_name = next(filter(lambda e: 'zip' in e, os.listdir(exp_dir)))
    _final_saf_file_name = _tmp_saf_file_name.replace('.processing', '')
    os.rename(os.path.join(exp_dir, _tmp_saf_file_name), 
              os.path.join(exp_dir, _final_saf_file_name))
    final_saf_path = str(os.path.join(exp_dir, _final_saf_file_name))
    shutil.unpack_archive(final_saf_path, extract_dir=exp_dir)

    # check contents file
    path_contents = os.path.join(exp_dir, 'item_000', 'contents')
    assert os.path.exists(path_contents)
    with open(path_contents, encoding='UTF-8') as cfh:
        all_lines = cfh.readlines()
        assert 69 == len(all_lines)
        contents_one_liner = ' '.join(all_lines)
        assert THE_METS in contents_one_liner
        assert "00000001.jpg\tbundle:MAX_IMAGE" in contents_one_liner
        assert "mets.xml\tbundle:FULLTEXT_OCR" not in contents_one_liner


def test_export_kitodo2_without_share_it_info_raises_exception(
        monography_319696111):
    """
    create export with monography data but invalid share_it info
    """

    # arrange
    (_, work_dir, exp_dir) = monography_319696111
    process_metafile = os.path.join(work_dir, KITODO2_MONOGRAPHY_METS)

    # act
    with pytest.raises(Exception) as exc:
        export_data_from(process_metafile, None, '319696111', 
            str(exp_dir), EXPORT_MAPPING_KITODO2)

    # assert
    assert 'No collections data provided' in str(exc.value)

    
def test_export_mappings_migration_monography(tmp_path):
    """Behavior with default matching paths
    bug fix monography 3207696
    """

    # arrange
    source_dir = os.path.join(TEST_RES, '320796')
    target_dir = tmp_path / 'WORKDIR'
    shutil.copytree(source_dir, target_dir)
    the_workdir = tmp_path / "export_workdir"
    the_workdir.mkdir()
    matching_paths = {'.xml': THE_METS, '.pdf': None, 'MAX': None}

    # act
    mapped_files = map_contents(target_dir, str(the_workdir), matching_paths)

    # assert
    assert len(mapped_files) == 17
    for mapped_file in mapped_files:
        last_src_segm = mapped_file.path_source.split('/')[-1]
        last_dst_segm = mapped_file.path_target.split('/')[-1]
        if last_dst_segm == THE_METS:
            continue
        assert last_src_segm == last_dst_segm


@pytest.fixture(name="re_ocr")
def _fixture_text_bundle(tmp_path):
    """arrange test setup for re-ocr with
    * 1 pdf
    * 1 pdf.txt
    * 10 xml files"""
    _working = tmp_path / 'WORKDIR'
    _working.mkdir()
    open(_working / FAKE_METS, mode='w').write('<mets></mets>')
    open(_working / FAKE_PDF, mode='wb').write(b'<mets></mets>')
    open(_working / f'{FAKE_PDF}.txt', mode='w').write('first line\nsecond line\nand third')
    _fulltext = _working / 'FULLTEXT'
    _fulltext.mkdir()
    _alto_txt = '<alto xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.loc.gov/standards/alto/ns-v4#" xsi:schemaLocation="http://www.loc.gov/standards/alto/ns-v4# http://www.loc.gov/standards/alto/v4/alto-4-1.xsd"></alto>'
    for i in range(1,11):
        open(_fulltext / f'{i:08d}.xml', mode='w').write(_alto_txt)
    _export_dst = tmp_path / 'export_dir'
    _export_dst.mkdir()
    return (_working, _export_dst)


def test_export_re_ocr_with_text_bundle_mappings(re_ocr):
    """Do we have proper export mappings yet?"""

    # arrange
    (_working, _export_dst) = re_ocr
    _xport_map = DEFAULT_EXPORT_MAPPINGS
    if '.xml' in _xport_map:
        del _xport_map['.xml']
    if '.pdf' in _xport_map:
        del _xport_map['.pdf']
    _xport_map[FAKE_METS] = THE_METS
    _xport_map[FAKE_PDF] = None
    _xport_map['.pdf.txt'] = None

    # act
    mappings = map_contents(_working, _export_dst, _xport_map)

    # assert 
    assert len(mappings) == 13


@pytest.fixture(name='re_ocr_saf_item000')
def test_export_re_ocr_with_text_bundle_contents_file(re_ocr):
    """Ensure additional bundle:TEXT will be
    part of generated contents information
    """

    # arrange
    (_working, _export_dst) = re_ocr
    _xport_map = DEFAULT_EXPORT_MAPPINGS
    if '.xml' in _xport_map:
        del _xport_map['.xml']
    if '.pdf' in _xport_map:
        del _xport_map['.pdf']
    _xport_map[FAKE_METS] = THE_METS
    _xport_map[FAKE_PDF] = None
    _xport_map['.pdf.txt'] = None
    
    # act
    export_data_from(
        _working / FAKE_METS, SHARE_IT_EXPORT_COLLECTION,
        export_map=_xport_map,
        saf_final_name='12345678', export_dst=_export_dst)
    
    # respect intermediate ".processing" - suffix
    # required for calling shutil - otherwise claims 
    # ".processing" is invalid archive format
    _tmp_saf_file_name = next(filter(lambda e: 'zip' in e, os.listdir(_export_dst)))
    _final_saf_file_name = _tmp_saf_file_name.replace('.processing', '')
    os.rename(os.path.join(_export_dst, _tmp_saf_file_name), 
              os.path.join(_export_dst, _final_saf_file_name))
    final_saf_path = str(os.path.join(_export_dst, _final_saf_file_name))
    shutil.unpack_archive(final_saf_path, extract_dir=_export_dst)
    yield os.path.join(_export_dst, 'item_000')


def test_export_re_ocr_files(re_ocr_saf_item000):
    """Assert the number of files makes sense:
    * 1 mets.xml
    * 10 OCR Files
    * 1 PDF
    * 1 Text-PDF
    * 4 SAF specifics: collections, contents, dublin_core.xml, metadata_local.xml
    """

    # check contents file

    # assert
    assert os.path.exists(os.path.join(re_ocr_saf_item000, THE_METS))
    assert os.path.exists(os.path.join(re_ocr_saf_item000, FAKE_PDF))
    assert os.path.exists(os.path.join(re_ocr_saf_item000, f'{FAKE_PDF}.txt'))
    assert os.path.exists(os.path.join(re_ocr_saf_item000, 'contents'))
    assert os.path.exists(os.path.join(re_ocr_saf_item000, 'collections'))
    assert os.path.exists(os.path.join(re_ocr_saf_item000, 'dublin_core.xml'))
    assert os.path.exists(os.path.join(re_ocr_saf_item000, 'contents'))
    assert 17 == len(os.listdir(re_ocr_saf_item000))


def test_export_re_ocr_files_content_file(re_ocr_saf_item000):
    """ensure content entries present"""    
    # arrange
    path_contents = os.path.join(re_ocr_saf_item000, 'contents')
    content_entries = [l.strip() for l in open(path_contents).readlines()]

    # assert
    assert 13 == len(content_entries)
    assert 'mets.xml\tbundle:METS_BACKUP' in content_entries
    assert FAKE_PDF in content_entries
    assert f'{FAKE_PDF}.txt\tbundle:TEXT' in content_entries
    assert '00000001.xml\tbundle:FULLTEXT_OCR' in content_entries


# split two-byte marks into single byte values
JPG_MARKS = [0xff, 0xd8, 0xff, 0xd9]
@pytest.fixture(name="monography_derivates")
def _fixture_mono_derivates(tmp_path):
    """Create data with images and additional
    Share_it derivates (Defaults, Thumbnails)
    which correspond the proper DSpace
    virtual file groups"""

    _working = tmp_path / 'WORKDIR'
    _working.mkdir()
    open(_working / FAKE_METS, mode='w').write('<mets></mets>')
    open(_working / FAKE_PDF, mode='wb').write(b'<mets></mets>')
    _dirs = ['IMAGE_FOOTER', 'BUNDLE_BRANDED_PREVIEW__', 'BUNDLE_THUMBNAIL__']
    for _d in _dirs:
        _path_d = _working / _d
        _path_d.mkdir()
        for i in range(1, 11):
            _img_path = f"{_d}{i:08}.jpg"
            # clear IMAGE_FOOTER, since this is not it's name
            # on the wildside (just like '00000001.jpg')
            _img_path = _img_path.replace('IMAGE_FOOTER', '')
            open(_path_d / _img_path, 'w+b').write(bytearray(JPG_MARKS))
    _export_dst = tmp_path / 'export_dir'
    _export_dst.mkdir()
    yield (_working, _export_dst)


def test_export_migration_derivates_mappings(monography_derivates):
    """Default Mappings as from VLS migrations"""

    # arrange
    (_src, _export_dst) = monography_derivates
    _xport_map = DEFAULT_EXPORT_MAPPINGS

    # act
    mappings = map_contents(_src, _export_dst, _xport_map)

    # assert 
    assert len(mappings) == 32


@pytest.fixture(name="monography_derivates_export")
def _fixture_monography_derivates_export(monography_derivates):

    # arrange
    (_src, _export_dst) = monography_derivates
    export_data_from(
        _src / FAKE_METS, SHARE_IT_EXPORT_COLLECTION,
        saf_final_name='12345678', export_dst=_export_dst)
    
    # respect intermediate ".processing" - suffix
    # required for calling shutil - otherwise claims 
    # ".processing" is invalid archive format
    _tmp_saf_file_name = next(filter(lambda e: 'zip' in e, os.listdir(_export_dst)))
    _final_saf_file_name = _tmp_saf_file_name.replace('.processing', '')
    os.rename(os.path.join(_export_dst, _tmp_saf_file_name), 
              os.path.join(_export_dst, _final_saf_file_name))
    final_saf_path = str(os.path.join(_export_dst, _final_saf_file_name))
    shutil.unpack_archive(final_saf_path, extract_dir=_export_dst)
    yield os.path.join(_export_dst, 'item_000')


def test_export_migration_derivates_files(monography_derivates_export):
    """Ensure all generated derivates are present
    The number of the beast: 36
    * 1 mets.xml
    * 10 MAX images, 10 Thumbnails, 10 default previews
    * 1 PDF
    * 4 SAF specifics: collections, contents, dublin_core.xml, metadata_local.xml
    """

    mde = monography_derivates_export

    # assert
    assert os.path.exists(os.path.join(mde, THE_METS))
    assert os.path.exists(os.path.join(mde, FAKE_PDF))
    assert os.path.exists(os.path.join(mde, 'contents'))
    assert os.path.exists(os.path.join(mde, 'collections'))
    assert os.path.exists(os.path.join(mde, 'dublin_core.xml'))
    assert os.path.exists(os.path.join(mde, 'contents'))
    assert os.path.exists(os.path.join(mde, '00000001.jpg'))
    assert os.path.exists(os.path.join(mde, 'BUNDLE_BRANDED_PREVIEW__00000001.jpg'))
    assert os.path.exists(os.path.join(mde, 'BUNDLE_THUMBNAIL__00000001.jpg'))
    assert 36 == len(os.listdir(mde))


def test_export_migration_derivates_contents_file(monography_derivates_export):
    """The contents of the contents^TM"""

    # arrange
    path_contents = os.path.join(monography_derivates_export, 'contents')
    content_entries = [l for l in open(path_contents).readlines()]
    _img1 = '00000001.jpg'

    # assert
    assert 12 == len(content_entries)
    assert 'mets.xml\tbundle:METS_BACKUP\n' in content_entries
    assert f"{FAKE_PDF}\n" in content_entries
    assert f'{_img1}\tbundle:MAX_IMAGE\tvirtual:{BP}{_img1}/preview;{BT}{_img1}/thumbnail\n' in content_entries
