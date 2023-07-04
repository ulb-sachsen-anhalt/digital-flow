"""
For the export of processes to share_it,
the generated data should be enriched and packed into a SAF.
The SAF file is a simple ZIP file,
but it is structured according to a specified structure.

Implements task #5735
"""

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

from lxml import etree as ET

from digiflow import (
    write_xml_file,
)


# default export mappings
SRC_FULLTEXT = 'FULLTEXT'
KITODO2_MAX = 'max'
SAF_METS_XML = 'mets.xml'
SRC_MAX = 'MAX'
SRC_IMAGE_FOOTER = 'IMAGE_FOOTER'
BUNDLE_PREVIEW = 'BUNDLE_BRANDED_PREVIEW__'
SRC_PREVIEW = 'BUNDLE_BRANDED_PREVIEW__'
BUNDLE_THUMBNAIL = 'BUNDLE_THUMBNAIL__'
SRC_THUMBS = 'BUNDLE_THUMBNAIL__'
DIR_IMAGES = [KITODO2_MAX, SRC_MAX, SRC_IMAGE_FOOTER]
DIR_DERIVATES = [SRC_PREVIEW, SRC_THUMBS]
DEFAULT_EXPORT_MAPPINGS = {'.xml': SAF_METS_XML,
                   '.pdf': None,
                   SRC_IMAGE_FOOTER: None,
                   SRC_THUMBS: None,
                   SRC_PREVIEW: None,
                   SRC_FULLTEXT: None
                   }
CONTENTS_FILE_MAPPINGS = {
    SRC_FULLTEXT : 'FULLTEXT_OCR',
    SRC_MAX : ''
}

class DigiFlowExportError(Exception):
    """Mark Export Exception"""


class ExportMapping:

    def __init__(self, path_source, path_target):
        if not os.path.exists(path_source):
            raise DigiFlowExportError("Invalid source path '{}'!'".format(path_source))
        if not os.path.isabs(path_source):
            raise DigiFlowExportError("Source path '{}' is not absolute!".format(path_source))
        path_target_dir = os.path.dirname(path_target)
        if not os.path.exists(path_target_dir):
            os.makedirs(path_target_dir)
        self.path_source = path_source
        self.path_target = path_target
        _path_src = pathlib.Path(path_source)
        self.src_bundle = _path_src.parent.stem

    def get(self):
        """Access mapping"""
        return (self.path_source, self.path_target)

    def copy(self):
        """copy from source to target"""
        shutil.copy(self.path_source, self.path_target)

    def __repr__(self) -> str:
        return f"{self.path_source} => {self.path_target}"

    def __eq__(self, __o: object) -> bool:
        return self.path_source == __o.path_source and self.path_target == __o.path_target
    
    def __lt__(self, __o: object) -> bool:
        self_src_base = os.path.basename(self.path_source)
        other_src_base = os.path.basename(__o.path_source)
        return self_src_base <= other_src_base


def process(export_mappings, work_dir, archive_name,
            target_collection, target_data_dir):
    """process digitalizates with images"""

    for mapping in export_mappings:
        mapping.copy()
    _handle_dublin_core_dummy(work_dir)
    _handle_dublin_core_derivates(work_dir)
    _handle_collections_file(work_dir, target_collection)
    #_handle_contents_file(work_dir)
    _handle_contents_file(work_dir, export_mappings)
    (the_tmp_path, the_filesize) = _compress(os.path.dirname(work_dir), archive_name)
    path_export_processing = _move_to_tmp_file(the_tmp_path, target_data_dir)
    return (path_export_processing, the_filesize)


def map_contents(src_dir, dst_dir, export_map=None):
    """
    Create Mappings for source dir file entries to 
    future destination dir entries.

    Serve as intermediate "SAF Building construction site".
    
    All matching entries equal or below source dir get copied 
    in flatten manner into default SAF tmp dir 'item_000'.
    If no explicite export-mapping provided, files are
    copied as-they-are, for example 
    
    Otherwise rename file accordingly. I.e., search for a
    file with extension '.xml' and turn it into 'mets.xml'
    """

    mappings = []
    if not export_map:
        export_map = {'.xml': SAF_METS_XML}

    source_paths = [os.path.join(curr, a_file)
                    for curr, _, files
                    in os.walk(src_dir)
                    for a_file in files]

    for src, dst in export_map.items():
        for source_path in source_paths:
            if src in source_path:
                # for each individual file review
                if not dst:
                    _default_dst = os.path.basename(source_path)
                    dst_path = os.path.join(dst_dir, _default_dst)
                else:
                    dst_path = os.path.join(dst_dir, dst)
                _mapping = ExportMapping(source_path, dst_path)
                # prevent re-addition
                if _mapping not in mappings:
                    mappings.append(_mapping)

    return mappings


# def _handle_contents_file(working_item_dir, only_mets=False):
def _handle_contents_file(working_item_dir, export_mappings):
    contents_file_path = os.path.join(working_item_dir, "contents")
    with open(contents_file_path, 'a', encoding='UTF-8') as contents_file:
        for mapping in sorted(export_mappings):
            _target_base = pathlib.Path(mapping.path_target)
            _name = _target_base.name
            _src_bundle = mapping.src_bundle
            if _name == SAF_METS_XML:
                contents_file.write(f"{SAF_METS_XML}\tbundle:METS_BACKUP\n")
            elif _name.endswith('.pdf'):
                contents_file.write(f"{_name}\n")
            elif _name.endswith('.pdf.txt'):
                contents_file.write(f"{_name}\tbundle:TEXT\n")
            elif _src_bundle == SRC_FULLTEXT and _is_alto(_target_base):
                contents_file.write(f"{_name}\tbundle:FULLTEXT_OCR\n")
            elif _src_bundle in DIR_IMAGES:
                image_row = _render_row(_name, working_item_dir)
                contents_file.write(image_row)
            # we know them, but we do *not* handle 'em
            # since they're already dealt at main image
            elif _src_bundle in DIR_DERIVATES:
                continue
            else:
                raise DigiFlowExportError(f"can't handle {mapping}!")


def _render_row(img_label, working_item_dir):
    """
    Render image entry in contents file
    extend with optional virtual derivates 
    for thumbnails and default images
    identified by their path segments

    Format
    1.jpg    bundle:MAX_IMAGE    virtual:BUNDLE_BRANDED_PREVIEW__1.jpg/preview;BUNDLE_THUMBNAIL__1.jpg/thumbnail
    """
    row = f"{img_label}\tbundle:MAX_IMAGE"
    all_jpgs = [_i for _i in os.listdir(working_item_dir) 
                if _i.endswith('.jpg')]
    previews = [_i for _i in all_jpgs if BUNDLE_PREVIEW in _i]
    thumbs = [_i for _i in all_jpgs if BUNDLE_THUMBNAIL in _i]
    if len(previews) > 0 and len(thumbs) > 0:
        row = f"{row}\tvirtual:{BUNDLE_PREVIEW}{img_label}/preview;{BUNDLE_THUMBNAIL}{img_label}/thumbnail"
    return row + "\n"


def _is_alto(path_file) -> bool:
    if not isinstance(path_file, str):
        path_file = str(path_file)
    if not path_file.endswith('.xml'):
        return False
    try:
        xml_tree = ET.parse(path_file).getroot()
        namespace = xml_tree.xpath('namespace-uri(.)')
        return "alto" in namespace.lower()
    except ET.ParseError as _err:
        raise DigiFlowExportError(f"Unknown OCR-Format: {_err.args}") from _err


def _compress(work_dir, archive_name):
    """
    Switched implementation since unable to de-compress zip64Format
    created with shutil.make_archive by Share_it
    """
    zip_size = -1
    zip_file_path = os.path.join(os.path.dirname(work_dir), archive_name) + '.zip'
    previous_dir = os.getcwd() 
    os.chdir(work_dir)
    cmd = 'zip -q -r {} item_000'.format(zip_file_path)
    subprocess.run(cmd, shell=True, check=True)
    os.chmod(zip_file_path, 0o666)
    zip_size = int(os.path.getsize(zip_file_path) / 1024 / 1024)
    os.chdir(previous_dir)
    return (zip_file_path, "{}MB".format(zip_size))


def _move_to_tmp_file(the_file_path, destination):
    """
    Move propably very large export data with masked name to external drive
    """
    abs_dstination = os.path.abspath(destination)
    zip_export_path = os.path.join(abs_dstination, os.path.basename(the_file_path))
    export_processing = zip_export_path + '.processing'
    if not os.path.isdir(abs_dstination):
        os.makedirs(abs_dstination, exist_ok=True)
    shutil.move(the_file_path, export_processing)
    return export_processing


def _handle_dublin_core_dummy(work_dir):
    dc_dummy_path = os.path.join(work_dir, "dublin_core.xml")
    dublin_core = ET.Element('dublin_core')
    el_title = ET.Element('dcvalue')
    el_title.set('element', 'title')
    el_title.set('qualifier', 'none')
    el_title.text = 'DUMMY'
    el_date = ET.Element('dcvalue')
    el_date.set('element', 'date')
    el_date.set('qualifier', 'issued')
    el_date.text = '1982'
    dublin_core.append(el_title)
    dublin_core.append(el_date)
    write_xml_file(dublin_core, dc_dummy_path)


def _handle_dublin_core_derivates(work_dir):
    """
    <?xml version="1.0" encoding="UTF-8"?>
    <dublin_core schema="local">
    <dcvalue element="picturegroup" qualifier="thumbnail">EXTERNAL</dcvalue>
    <dcvalue element="picturegroup" qualifier="preview">EXTERNAL</dcvalue>
    </dublin_core>
    """
    dc_path = os.path.join(work_dir, "metadata_local.xml")
    dublin_core = ET.Element('dublin_core', {'schema': 'local'})
    el_pict = ET.Element('dcvalue')
    el_pict.set('element', 'picturegroup')
    el_pict.set('qualifier', 'thumbnail')
    el_pict.text = 'EXTERNAL'
    el_prev = ET.Element('dcvalue')
    el_prev.set('element', 'picturegroup')
    el_prev.set('qualifier', 'preview')
    el_prev.text = 'EXTERNAL'
    dublin_core.append(el_pict)
    dublin_core.append(el_prev)
    write_xml_file(dublin_core, dc_path)


def _handle_collections_file(work_dir, collections):
    if collections:
        collections_path = os.path.join(work_dir, "collections")
        with open(collections_path, 'a') as collections_file:
            collections_file.write(collections)
    else:
        raise DigiFlowExportError("No collections data provided - invalid share_it export!")


def export_data_from(process_metafile_path,
                     collection,
                     saf_final_name,
                     export_dst,
                     export_map=DEFAULT_EXPORT_MAPPINGS,
                     tmp_saf_dir=None):
    """
    Main entry point to prepare, create and export specified data
    related to provided digitalization item process metadatafile_path
    """

    source_path_dir = os.path.dirname(process_metafile_path)
    tmp_dir = tempfile.gettempdir()
    prefix = 'opendata-working-'
    if tmp_saf_dir:
        tmp_dir = tmp_saf_dir
    with tempfile.TemporaryDirectory(prefix=prefix, dir=tmp_dir) as tmp_dir:
        work_dir = os.path.join(tmp_dir, saf_final_name, 'item_000')
        _mappings = map_contents(source_path_dir, work_dir, export_map)
        return process(_mappings, work_dir, saf_final_name, collection, export_dst)
