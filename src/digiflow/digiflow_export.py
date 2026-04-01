"""
Provides the functionality to export a single digital object
from digitalization workflows as item_0000 suitable for DSpace.

The format is SAF (Submission Archive Format), which is a simple ZIP file,
but structured according to a specified structure, containing
* a METS file, which describes the digital object,
* a contents file, which lists the files in the archive,
* a dublin_core.xml file, which acts as placeholder for DSpace
* and the actual files of the digital object, such as images, PDFs, etc.

Dependencies:
- lxml
- zipfile

Implements task #5735
"""

import dataclasses
import os
import pathlib
import shutil
import subprocess
import tempfile
import typing

import lxml.etree as ET

from digiflow import (
    write_xml_file,
)

# default export mappings
SRC_FULLTEXT = "FULLTEXT"
KITODO2_MAX = "max"
SAF_METS_XML = "mets.xml"
SRC_MAX = "MAX"
SRC_IMAGE_FOOTER = "IMAGE_FOOTER"
BUNDLE_PREVIEW = "BUNDLE_BRANDED_PREVIEW__"
SRC_PREVIEW = "BUNDLE_BRANDED_PREVIEW__"
BUNDLE_THUMBNAIL = "BUNDLE_THUMBNAIL__"
SRC_THUMBS = "BUNDLE_THUMBNAIL__"
DIR_IMAGES = [KITODO2_MAX, SRC_MAX, SRC_IMAGE_FOOTER]
DIR_DERIVATES = [SRC_PREVIEW, SRC_THUMBS]
ULB_DEFAULT_EXPORT_MAPPINGS = {
    ".xml": SAF_METS_XML,
    ".pdf": "",
    SRC_IMAGE_FOOTER: "",
    SRC_THUMBS: "",
    SRC_PREVIEW: "",
    SRC_FULLTEXT: "",
}
CONTENTS_FILE_MAPPINGS = {SRC_FULLTEXT: "FULLTEXT_OCR", SRC_MAX: ""}

EXPORT_CMD_PATTERN = "zip -q -r {} item_000"

SAF_ADDITIONAL_DUBLIN_CORE = "dublin_core"
SAF_ADDITIONAL_DUBLIN_CORE_DERIVATES = "dublin_core_derivates"
SAF_ADDITIONAL_COLLECTIONS = "collections"
SAF_ADDITIONAL_CONTENTS = "contents"
DEFAULT_SAF_ADDITIONAL_FILES = [
    SAF_ADDITIONAL_DUBLIN_CORE,
    SAF_ADDITIONAL_DUBLIN_CORE_DERIVATES,
    SAF_ADDITIONAL_COLLECTIONS,
    SAF_ADDITIONAL_CONTENTS,
]
ALLOWED_SAF_ADDITIONAL_FILES = set(DEFAULT_SAF_ADDITIONAL_FILES)


def _validate_saf_additional_files(saf_additional_files):
    """Validate optional SAF additional file selector values."""
    if saf_additional_files is None:
        return list(DEFAULT_SAF_ADDITIONAL_FILES)
    if not isinstance(saf_additional_files, list):
        raise DigiFlowExportError(
            "invalid saf_additional_files: expected list of SAF flags."
        )
    unknown_files = sorted(set(saf_additional_files) - ALLOWED_SAF_ADDITIONAL_FILES)
    if unknown_files:
        allowed = sorted(ALLOWED_SAF_ADDITIONAL_FILES)
        raise DigiFlowExportError(
            f"unknown SAF additional files requested: {unknown_files}. Allowed: {allowed}"
        )
    return list(saf_additional_files)


class DigiFlowExportError(Exception):
    """Mark Export Exception"""


class SourceFileMapping:
    """Map a source file with absolute path to a target path,
    which may be a directory or a file.
    May contain optional information about access rights
    or DSpace-SAF bundle information, i.e. "MAX", or "FULLTEXT".

    If bundle information not provided, it will be derived
    from source file's path subdirectory, i.e. "<base_dir>/MAX".
    """

    def __init__(self, path_source: pathlib.Path, path_target: pathlib.Path,
                 access_right=None, dspace_bundle=None):
        if not isinstance(path_source, pathlib.Path):
            path_source = pathlib.Path(path_source)
        if not isinstance(path_target, pathlib.Path):
            path_target = pathlib.Path(path_target)
        if not path_source.is_absolute():
            raise DigiFlowExportError(f"source path '{path_source}' not absolute!")
        if not path_source.exists():
            raise DigiFlowExportError(f"invalid source path '{path_source}'!")
        path_target_dir = path_target.parent

        if not path_target_dir.exists():
            os.makedirs(path_target_dir)
        self.path_source = path_source
        self.path_target = path_target
        self.access_right = access_right
        self.dspace_bundle = dspace_bundle
        if self.dspace_bundle is None:
            path_src = pathlib.Path(path_source)
            self.dspace_bundle = path_src.parent.stem

    def copy(self):
        """copy from source to target"""
        shutil.copy(self.path_source, self.path_target)

    def __eq__(self, other: object) -> bool:
        """Enable check with in-operator to prevent duplicate ExportMappings."""
        if not isinstance(other, SourceFileMapping):
            return NotImplemented
        return self.path_source == other.path_source and self.path_target == other.path_target

    def __lt__(self, other: object) -> bool:
        """Enable meaningful sorting of ExportMappings by source path basename."""
        if not isinstance(other, SourceFileMapping):
            return NotImplemented
        self_src_base = os.path.basename(self.path_source)
        other_src_base = os.path.basename(other.path_source)
        return self_src_base <= other_src_base


@dataclasses.dataclass(frozen=True)
class ExportRequest:
    """Value object that contains all arguments needed for a single export run."""

    process_metafile_path: pathlib.Path
    saf_final_name: str
    export_dst: typing.Optional[pathlib.Path] = None
    export_map: typing.Optional[dict] = None
    collection: typing.Optional[str] = None
    tmp_saf_dir: typing.Optional[pathlib.Path] = None


@dataclasses.dataclass
class DigiFlowExporterConfig:
    """Optional runtime configuration for DigiFlowExporter."""

    mapping_factory: typing.Optional[typing.Callable] = None
    processor: typing.Optional[typing.Callable] = None
    workspace_cls: typing.Optional[type] = None
    default_export_map: typing.Optional[dict] = None
    saf_additional_files: typing.Optional[typing.List[str]] = None


class SingleItemExportWorkspace:
    """Create temporary SAF export workspace."""

    TMP_DIR_PREFIX = "export-working-"
    ITEM_SUB_DIR = "item_000"

    def __init__(self, saf_final_name, item_sub_dir=ITEM_SUB_DIR,
                 tmp_saf_dir=None):
        self.saf_final_name = saf_final_name
        self.single_item_sub_dir = item_sub_dir
        self.tmp_saf_dir: typing.Optional[pathlib.Path] = tmp_saf_dir
        self.tmp_dir: typing.Optional[str] = None
        self.item_dir: typing.Optional[pathlib.Path] = None
        self._context = None

    def __enter__(self):
        temp_root = tempfile.gettempdir()
        if self.tmp_saf_dir:
            temp_root = self.tmp_saf_dir
        self._context = tempfile.TemporaryDirectory(prefix=self.TMP_DIR_PREFIX, dir=temp_root)
        self.tmp_dir = self._context.__enter__()
        self.item_dir = pathlib.Path(self.tmp_dir) / self.saf_final_name / self.single_item_sub_dir
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self._context is not None, "ExportWorkspace context not properly initialized!"
        return self._context.__exit__(exc_type, exc_val, exc_tb)


class SAFExporter:
    """Application SAF export for digitalisation data of 
    * METS/MODS, and
    * opt. Images (TIFF, JPEG)
    * opt. OCR (ALTO XML)
    * opt. PDFs (born-digital or scanned)
    Includes export logic from given workspace, building the SAF structure with all
    expected files and DSpace metadata assets, and processing the export,
    i.e. creating a Zip archive and moving this to optional target destination.
    """

    def __init__(
        self,
        mapping_factory=None,
        processor=None,
        config: typing.Optional[DigiFlowExporterConfig] = None,
    ):
        cfg_mapping_factory = config.mapping_factory if config else None
        cfg_processor = config.processor if config else None
        cfg_default_export_map = config.default_export_map if config else None
        cfg_saf_additional_files = config.saf_additional_files if config else None
        self.mapping_func = mapping_factory or cfg_mapping_factory or map_contents
        self.processor = processor or cfg_processor or process
        self.default_export_map = cfg_default_export_map or ULB_DEFAULT_EXPORT_MAPPINGS
        self.saf_additional_files = _validate_saf_additional_files(
            cfg_saf_additional_files
        )

    def run(self, request: ExportRequest):
        """Run export with given request parameters."""
        source_path_dir = self._resolve_source_path_dir(request.process_metafile_path)
        export_map = request.export_map
        if export_map is None:
            export_map = self.default_export_map
        with SingleItemExportWorkspace(saf_final_name=request.saf_final_name,
                             tmp_saf_dir=request.tmp_saf_dir) as workspace:
            assert workspace.item_dir is not None, "ExportWorkspace item_dir invalid!"
            mappings = self.mapping_func(source_path_dir, workspace.item_dir, export_map)
            return self.processor(
                mappings,
                workspace.item_dir,
                request.saf_final_name,
                request.collection,
                request.export_dst,
                self.saf_additional_files,
            )

    def move(self, the_file_path, destination):
        """Move propably very large export artefact with intermediate suffix to external drive."""
        return move_file_to(the_file_path, destination)

    @staticmethod
    def _resolve_source_path_dir(process_metafile_path):
        source_path_dir = os.path.dirname(process_metafile_path)
        if not os.path.exists(source_path_dir):
            raise DigiFlowExportError(
                f"Source directory does not exist: {source_path_dir}"
            )
        return source_path_dir


def process(
    export_mappings,
    work_dir,
    archive_name,
    target_collection,
    target_data_dir,
    saf_additional_files=None,
):
    """process digitalizates with images"""

    selected_files = set(_validate_saf_additional_files(saf_additional_files))

    for mapping in export_mappings:
        mapping.copy()
    if SAF_ADDITIONAL_DUBLIN_CORE in selected_files:
        _generate_dublin_core_file(work_dir)
    if SAF_ADDITIONAL_DUBLIN_CORE_DERIVATES in selected_files:
        _generate_dublin_core_derivates(work_dir)
    if SAF_ADDITIONAL_COLLECTIONS in selected_files:
        _handle_collections_file(work_dir, target_collection)
    if SAF_ADDITIONAL_CONTENTS in selected_files:
        _generate_contents_file(work_dir, export_mappings)
    the_tmp_path, the_filesize = compress(os.path.dirname(work_dir), archive_name)
    path_export_processing = move_file_to(the_tmp_path, target_data_dir)
    return (path_export_processing, the_filesize)


def map_contents(src_dir: pathlib.Path, dst_dir: pathlib.Path,
                 export_map=None) -> typing.List[SourceFileMapping]:
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

    mappings: typing.List[SourceFileMapping] = []
    access_right = None
    if not export_map:
        export_map = {".xml": SAF_METS_XML}

    source_paths = [
        os.path.join(curr, a_file)
        for curr, _, files in os.walk(src_dir)
        for a_file in files
    ]

    for src, dst in export_map.items():
        # for each individual file review
        for source_path in source_paths:
            if src in source_path:
                # if isinstance(dst, list):
                #     # with E-Pflicht migr. we pass the right
                #     # as second list element
                #     dst, access_right = dst
                if not dst or len(dst.strip()) == 0:
                    default_dst = os.path.basename(source_path)
                    dst_path = os.path.join(dst_dir, default_dst)
                else:
                    dst_path = os.path.join(dst_dir, dst)
                sf_mapping = SourceFileMapping(pathlib.Path(source_path),
                                               pathlib.Path(dst_path),
                                               access_right=access_right)
                # prevent re-addition
                if sf_mapping not in mappings:
                    mappings.append(sf_mapping)

    return mappings


def _generate_contents_file(working_item_dir,
                          export_mappings: typing.List[SourceFileMapping]):
    contents_file_path = os.path.join(working_item_dir, "contents")
    with open(contents_file_path, "a", encoding="UTF-8") as contents_file:
        for mapping in sorted(export_mappings):
            target_base = pathlib.Path(mapping.path_target)
            the_name = target_base.name
            src_bundle = mapping.dspace_bundle
            if the_name == SAF_METS_XML:
                contents_file.write(f"{SAF_METS_XML}\tbundle:METS_BACKUP\n")
            elif the_name.endswith((".pdf", ".epub")):
                the_right = ""
                if mapping.access_right is not None:
                    the_right = f"\tpermissions: -r {mapping.access_right}"
                contents_file.write(f"{the_name}{the_right}\n")
            elif the_name in ["dublin_core.xml", "relationships"] or the_name.startswith(
                "metadata_"
            ):
                # these xml's are dublin core metadata for E-Pflicht migration
                continue
            elif the_name.endswith(".pdf.txt"):
                contents_file.write(f"{the_name}\tbundle:TEXT\n")
            elif src_bundle == SRC_FULLTEXT and _is_alto(target_base):
                contents_file.write(f"{the_name}\tbundle:FULLTEXT_OCR\n")
            elif src_bundle in DIR_IMAGES:
                image_row = _render_content_file_row(the_name, working_item_dir)
                contents_file.write(image_row)
            # we know them, but we do *not* handle 'em
            # since they're already dealt at main image
            elif src_bundle in DIR_DERIVATES:
                continue
            else:
                raise DigiFlowExportError(f"can't handle {mapping}!")


def _render_content_file_row(img_label, working_item_dir):
    """
    Render image entry in contents file
    extend with optional virtual derivates
    for thumbnails and default images
    identified by their path segments

    Format
    1.jpg    bundle:MAX_IMAGE    virtual:BUNDLE_BRANDED_PREVIEW__1.jpg/preview;BUNDLE_THUMBNAIL__1.jpg/thumbnail
    """
    row = f"{img_label}\tbundle:MAX_IMAGE"
    all_jpgs = [_i for _i in os.listdir(working_item_dir) if _i.endswith(".jpg")]
    previews = [_i for _i in all_jpgs if BUNDLE_PREVIEW in _i]
    thumbs = [_i for _i in all_jpgs if BUNDLE_THUMBNAIL in _i]
    if len(previews) > 0 and len(thumbs) > 0:
        row = f"{row}\tvirtual:{BUNDLE_PREVIEW}{img_label}/preview;{BUNDLE_THUMBNAIL}{img_label}/thumbnail"
    return row + "\n"


def _is_alto(path_file) -> bool:
    if not isinstance(path_file, str):
        path_file = str(path_file)
    if not path_file.endswith(".xml"):
        return False
    try:
        xml_tree = ET.parse(path_file).getroot()
        namespace = xml_tree.xpath("namespace-uri(.)")
        return "alto" in namespace.lower()
    except ET.ParseError as _err:
        raise DigiFlowExportError(f"Unknown OCR-Format: {_err.args}") from _err


def compress(work_dir, archive_name):
    """
    Switched implementation since unable to de-compress zip64Format
    created with shutil.make_archive by Share_it
    """
    zip_size = -1
    zip_file_path = os.path.join(os.path.dirname(work_dir), archive_name) + ".zip"
    previous_dir = os.getcwd()
    os.chdir(work_dir)
    cmd = EXPORT_CMD_PATTERN.format(zip_file_path)
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        raise DigiFlowExportError(f"Export subprocess failed: {e}") from e
    os.chmod(zip_file_path, 0o666)
    zip_size = int(os.path.getsize(zip_file_path) / 1024 / 1024)
    os.chdir(previous_dir)
    return (zip_file_path, f"{zip_size}MB")


def move_file_to(the_file_path, destination):
    """
    Move propably very large export artefact with intermediate suffix to external drive
    """
    abs_dstination = os.path.abspath(destination)
    zip_export_path = os.path.join(abs_dstination, os.path.basename(the_file_path))
    export_processing = pathlib.Path(zip_export_path).with_suffix(".processing")
    if not os.path.isdir(abs_dstination):
        os.makedirs(abs_dstination, exist_ok=True)
    # this step may take mminutes 'til midnight
    shutil.move(the_file_path, export_processing)
    # finalize export by renaming to final name without .processing suffix
    final_export_path = pathlib.Path(export_processing).with_suffix(".zip")
    os.rename(export_processing, final_export_path)
    return final_export_path


def _generate_dublin_core_file(work_dir):
    dc_dummy_path = os.path.join(work_dir, "dublin_core.xml")
    if os.path.exists(dc_dummy_path):
        # already provided by pipeline (E-Pflicht)
        return
    dublin_core = ET.Element("dublin_core")
    el_title = ET.Element("dcvalue")
    el_title.set("element", "title")
    el_title.set("qualifier", "none")
    el_title.text = "DUMMY"
    el_date = ET.Element("dcvalue")
    el_date.set("element", "date")
    el_date.set("qualifier", "issued")
    el_date.text = "1982"
    dublin_core.append(el_title)
    dublin_core.append(el_date)
    write_xml_file(dublin_core, dc_dummy_path)


def _generate_dublin_core_derivates(work_dir):
    """
    <?xml version="1.0" encoding="UTF-8"?>
    <dublin_core schema="local">
    <dcvalue element="picturegroup" qualifier="thumbnail">EXTERNAL</dcvalue>
    <dcvalue element="picturegroup" qualifier="preview">EXTERNAL</dcvalue>
    </dublin_core>
    """
    dc_path = os.path.join(work_dir, "metadata_local.xml")
    if os.path.exists(dc_path):
        # already provided by pipeline (EPflicht)
        return
    dublin_core = ET.Element("dublin_core", {"schema": "local"})
    el_pict = ET.Element("dcvalue")
    el_pict.set("element", "picturegroup")
    el_pict.set("qualifier", "thumbnail")
    el_pict.text = "EXTERNAL"
    el_prev = ET.Element("dcvalue")
    el_prev.set("element", "picturegroup")
    el_prev.set("qualifier", "preview")
    el_prev.text = "EXTERNAL"
    dublin_core.append(el_pict)
    dublin_core.append(el_prev)
    write_xml_file(dublin_core, dc_path)


def _handle_collections_file(work_dir, collections):
    if collections:
        collections_path = os.path.join(work_dir, "collections")
        with open(collections_path, "a", encoding="UTF-8") as collections_file:
            collections_file.write(collections)

def export_data_from(
    process_metafile_path,
    saf_final_name,
    export_dst,
    collection=None,
    export_map=None,
    tmp_saf_dir=None,
):
    """
    Main entry point to prepare, create and export specified data
    related to provided digitalization item process metadatafile_path
    """
    request = ExportRequest(
        process_metafile_path=process_metafile_path,
        saf_final_name=saf_final_name,
        export_dst=export_dst,
        collection=collection,
        export_map=export_map,
        tmp_saf_dir=tmp_saf_dir,
    )
    return SAFExporter().run(request)
