"""Validate data on file-level"""

import datetime
import io
import os
import stat

from dataclasses import (
    dataclass,
)
from hashlib import (
    sha512
)
from pathlib import (
    Path,
)

from PIL import (
    Image as PILImage,
    ImageCms,
)
from PIL.TiffImagePlugin import (
    IMAGEWIDTH,     # 256
    IMAGELENGTH,    # 257
    BITSPERSAMPLE,  # 258 (8,8,8)=8 Bit_depth per channel
    COMPRESSION,    # 259 1=uncompressed
    PHOTOMETRIC_INTERPRETATION,  # 262 1=BlackIsZero, 2=RGB
    SAMPLESPERPIXEL,    # 277 1=greyscale, 3=3-channel RGB
    X_RESOLUTION,       # 282
    Y_RESOLUTION,       # 283
    RESOLUTION_UNIT,    # 296 2=Inches,3=cm
    SOFTWARE,       # 305
    DATE_TIME,      # 306
    ARTIST,         # 315
    COPYRIGHT,      # 33423
    ICCPROFILE,     # 34675
    ImageFileDirectory_v2,
    TiffImageFile,
    IFDRational,
)
# special tag
MODEL = 272  # EXIF 272

PERMISSION_GROUP_READ = stat.S_IRGRP
PERMISSION_GROUP_READ_WRITE = [stat.S_IRGRP, stat.S_IWGRP]

UNSET_LABEL = 'n.a.'
UNSET_NUMBR = -1
MAX_CHANNEL_DEPTH = 8
DEFAULT_COMPRESSION = 1     # EXIF 259 1=uncompressed
PHOTOMETRICS = [1, 2]       # EXIF 262 1 = BlackIsZero, 2 = rgb
GREYSCALE_OR_RGB = [1, 3]   # EXIF 277 1 = greyscale, 3 = 3-channel RGB
RES_UNIT_DPI = 2
ADOBE_PROFILE_NAME = 'Adobe RGB (1998)'  # Preferred ICC-Profile for RGB-TIF
DATETIME_MIX_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATETIME_SRC_FORMAT = "%Y:%m:%d %H:%M:%S"


# tied to actual derivans implementation
DEFAULT_REQUIRED_TIFF_METADATA = [
    IMAGEWIDTH,
    IMAGELENGTH,
    BITSPERSAMPLE,
    COMPRESSION,
    SAMPLESPERPIXEL,
    X_RESOLUTION,
    Y_RESOLUTION,
    RESOLUTION_UNIT,
    ICCPROFILE,
]
TIFF_METADATA_LABELS = {
    IMAGEWIDTH: 'width',
    IMAGELENGTH: 'height',
    BITSPERSAMPLE: 'channel',
    X_RESOLUTION: 'xRes',
    Y_RESOLUTION: 'yRes',
    RESOLUTION_UNIT: 'resolution_unit',
    ARTIST: 'artist',
    COMPRESSION: 'compression',
    COPYRIGHT: 'copyright',
    MODEL: 'model',
    SOFTWARE: 'software',
    DATE_TIME: 'created',
    SAMPLESPERPIXEL: 'samples_per_pixel',
    PHOTOMETRIC_INTERPRETATION: 'photometric_interpretation',
    ICCPROFILE: 'icc_profile'
}


class FSReadException(Exception):
    """Mark State process can't even 
    read file resource due ownership
    """


class FSWriteException(Exception):
    """Mark state process tries to write
    resource for modifications and/or
    sanitizing but isn't allowed
    to store data due ownership 
    restrictions"""


class ImageInvalidException(Exception):
    """Mark invalid image data"""


@dataclass
class ImageMetadata:
    """Wrap image metadata information (i.e. EXIF)
    """

    width = 0
    height = 0
    compression = UNSET_NUMBR
    photometric_interpretation = UNSET_NUMBR
    samples_per_pixel = UNSET_NUMBR
    model = UNSET_LABEL
    xRes = UNSET_NUMBR
    yRes = UNSET_NUMBR
    resolution_unit = UNSET_NUMBR
    color_space = UNSET_NUMBR
    artist = UNSET_LABEL
    copyright = UNSET_LABEL
    software = UNSET_LABEL
    created = UNSET_LABEL  # EXIF TAG 306 DATE_TIME
    channel = UNSET_NUMBR  # EFIX TAG 258 BITSPERSAMPLE


@dataclass
class Image:
    """Store required data"""

    local_path: str
    url = UNSET_LABEL
    file_size = 0
    time_stamp = None
    profile = UNSET_LABEL
    image_checksum = UNSET_LABEL
    metadata: ImageMetadata = None
    invalids = []

    def read(self):
        """Try to read image data"""
        try:
            _ts_object = datetime.datetime.now()
            self.time_stamp = _ts_object.strftime(DATETIME_SRC_FORMAT)
            # open resource
            _pil_img: TiffImageFile = PILImage.open(self.local_path)
            _image_bytes = _pil_img.tobytes()
            self.image_checksum = sha512(_image_bytes).hexdigest()

            # read information from TIF TAG V2 section
            _meta_data: ImageMetadata = self._read(_pil_img)
            _meta_data.color_space = _pil_img.mode

            # datetime present?
            if _meta_data.created is None or _meta_data.created == UNSET_LABEL:
                ctime = os.stat(self.local_path).st_ctime
                dt_object = datetime.datetime.fromtimestamp(ctime)
                _meta_data.created = dt_object.strftime(DATETIME_SRC_FORMAT)
            self.metadata = _meta_data
            _pil_img.close()
        except Exception as exc:
            raise ImageInvalidException(exc.args[0])
    
    def _read(self, img_data) -> ImageMetadata:
        """Read present TIFF metadata information
        Please note, ICC Profile requires special
        care since this is included in bytes
        """
        
        _image_tags: ImageFileDirectory_v2 = img_data.tag_v2
        _image_md = ImageMetadata()
        for _tag in _image_tags:
            if _tag in TIFF_METADATA_LABELS:
                _label = TIFF_METADATA_LABELS[_tag]
                if _tag != ICCPROFILE:
                    _val = _image_tags[_tag]
                    if isinstance(_val, str):
                        _val = str(_val).strip()
                    setattr(_image_md, _label, _val)
                else:
                    _profile = img_data.info.get('icc_profile')
                    if _profile:
                        _profile_data = io.BytesIO(_profile)
                        _profile_cms = ImageCms.ImageCmsProfile(_profile_data)
                        if _profile_cms:
                            _profile_name = ImageCms.getProfileDescription(_profile_cms)
                            if _profile_name:
                                _stripped = _profile_name.strip()
                            self.profile = _stripped
        return _image_md
    
    def validate(self, required_tags):
        """Inspect data - validate set of 
        properties for every single image"""

        _invalids = []

        # all required information set?
        _missing_tags = []
        for _tag in required_tags:
            _label = TIFF_METADATA_LABELS[_tag]
            _current = getattr(self.metadata, _label, UNSET_LABEL)
            if not _current:
                _missing_tags.append(_label)        
        if len(_missing_tags) > 0:
            _invalids.append(_missing_tags)

        # dimension
        _imd = self.metadata
        if not self.metadata.width or not self.metadata.height:
            _invalids.append("no width/height")

        # compression
        if _imd.compression != 1:
            _invalids.append(TIFF_METADATA_LABELS[COMPRESSION])

        # samples per pixel
        _ssp = _imd.samples_per_pixel
        if _ssp == UNSET_LABEL:
            _invalids.append(f"{TIFF_METADATA_LABELS[SAMPLESPERPIXEL]}: {UNSET_LABEL}")
        elif _ssp not in GREYSCALE_OR_RGB:
            _invalids.append(f"{TIFF_METADATA_LABELS[SAMPLESPERPIXEL]}: {_ssp}")

        # for each channel depth *must* not be larger than 8bit
        if not all(c <= 8 for c in _imd.channel):
            _invalids.append(f"{TIFF_METADATA_LABELS[BITSPERSAMPLE]}: {_imd.channel}")

        # resolution
        if _imd.resolution_unit != RES_UNIT_DPI:
            _invalids.append(TIFF_METADATA_LABELS[RESOLUTION_UNIT])
        if _imd.xRes == 0:
            _invalids.append(f"{TIFF_METADATA_LABELS[X_RESOLUTION]}: 0")
        if _imd.yRes == 0:
            _invalids.append(f"{TIFF_METADATA_LABELS[Y_RESOLUTION]}: 0")
        _xres: IFDRational = _imd.xRes
        if _xres.real.denominator != 1:
            _invalids.append(f"{TIFF_METADATA_LABELS[X_RESOLUTION]}: {_imd.xRes}")
        _yres: IFDRational = _imd.yRes
        if _yres.real.denominator != 1:
            _invalids.append(f"{TIFF_METADATA_LABELS[Y_RESOLUTION]}: {_imd.yRes}")

        # photometric interpretation
        _pmetric = _imd.photometric_interpretation
        if _pmetric == 2 and self.profile != ADOBE_PROFILE_NAME:
            _invalids.append(f"unexpected profile: {_pmetric}")
        elif _pmetric not in PHOTOMETRICS:
            _label = TIFF_METADATA_LABELS[PHOTOMETRIC_INTERPRETATION]
            _invalids.append(f"{_label} must be in {PHOTOMETRICS}: {_pmetric}")

        if len(_invalids) > 0:
            self.invalids = _invalids
        return _invalids

    def __str__(self):
        _fsize = f"{self.file_size}"
        _md = self.metadata
        _mds_res = f"({_md.xRes},{_md.yRes},{_md.resolution_unit})"
        _mds = f"\t{_mds_res}\t{_md.created}\t{_md.artist}\t{_md.copyright}\t{_md.model}\t{_md.software}\t{self.profile}"
        _invalids = f"INVALID{self.invalids}" if len(
            self.invalids) > 0 else 'VALID'
        return f"{self.url}\t{self.image_checksum}\t{_fsize}\t{self.metadata.width}x{self.metadata.height}\t{_mds}\t{self.time_stamp}\t{_invalids}"


def _check_modus(res_path, fs_modi=None):
    """filesystem provides detailed
    information about access permissions
    check on bit-level where *all* requested
    file modi *must* match
    """

    if isinstance(res_path, str):
        res_path = Path(res_path)
    stat_result = res_path.stat()
    _fs_modi = stat_result.st_mode
    if fs_modi is None:
        fs_modi = [PERMISSION_GROUP_READ]
    return all(_fs_modi & m for m in fs_modi)


def resource_can_be(res_path, modi):
    """Check whether resource can
    be read/write/executed as required
    """

    return _check_modus(res_path, modi)


def group_can_read(res_path) -> bool:
    """Inspect whether resource can be read
    at least"""

    return resource_can_be(res_path, [PERMISSION_GROUP_READ])


def group_can_write(res_path) -> bool:
    """Inspect whether resouce could be written
    in case to persist modifications"""

    return resource_can_be(res_path, PERMISSION_GROUP_READ_WRITE)


def validate_tiff(tif_path, required_metadata=None):
    """Ensure provided image data contains
    required metadata information concerning
    resolution and alike
    """

    if not group_can_read(tif_path):
        raise FSReadException(f"Group not permitted to read {tif_path}!")
    if required_metadata is None:
        required_metadata = DEFAULT_REQUIRED_TIFF_METADATA
    _file_size = os.path.getsize(tif_path)
    if _file_size < 1024:
        msg = f"Invalid filesize {_file_size} Bytes for {tif_path}!"
        raise FSReadException(msg)
    _img_data = Image(tif_path)
    _img_data.url = tif_path
    _img_data.file_size = _file_size
    _img_data.read()
    _invalids = _img_data.validate(required_metadata)
    if len(_invalids) > 0:
        _msg = f"{tif_path} invalid: {_invalids}"
        raise ImageInvalidException(_msg)
    return _img_data
