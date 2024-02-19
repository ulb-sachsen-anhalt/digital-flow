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

UNSET = 'n.a.'
DEFAULT_COMPRESSION = 1     # EXIF 259 1=uncompressed
PHOTOMETRICS = [1, 2]       # EXIF 262 1 = BlackIsZero, 2 = rgb
GREYSCALE_OR_RGB = [1, 3]   # EXIF 277 1 = greyscale, 3 = 3-channel RGB
RES_UNIT_DPI = 2
ADOBE_PROFILE_NAME = 'Adobe RGB (1998)'  # Preferred ICC-Profile for RGB-TIF
DATETIME_MIX_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATETIME_SRC_FORMAT = "%Y:%m:%d %H:%M:%S"


# tied to actual derivans implementation
DEFAULT_REQUIRED_TIFF_TAGS = [
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
TAG_LABELS = {
    IMAGEWIDTH: 'width',
    IMAGELENGTH: 'height',
    X_RESOLUTION: 'xRes',
    Y_RESOLUTION: 'yRes',
    RESOLUTION_UNIT: 'unit',
    ARTIST: 'artist',
    COMPRESSION: 'compression',
    COPYRIGHT: 'copyright',
    MODEL: 'model',
    SOFTWARE: 'software',
    DATE_TIME: 'created',
    SAMPLESPERPIXEL: 'samples_per_pixel',
    BITSPERSAMPLE: 'channel',
    PHOTOMETRIC_INTERPRETATION: 'photometric_interpretation',
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
    compression = UNSET
    photometric_interpretation = UNSET
    samples_per_pixel = UNSET
    model = UNSET
    xRes = UNSET
    yRes = UNSET
    resolution_unit = UNSET
    color_space = UNSET
    artist = UNSET
    copyright = UNSET
    software = UNSET
    created = UNSET  # EXIF TAG 306 DATE_TIME
    channel = UNSET  # EFIX TAG 258 BITSPERSAMPLE


@dataclass
class Image:
    """Store required data"""

    local_path: str
    url = UNSET
    file_size = 0
    time_stamp = None
    profile = UNSET
    image_checksum = UNSET
    metadata: ImageMetadata = None
    invalids = []

    def validate(self):
        """Inspect data - validate set of 
        properties for every single image"""

        # dimension
        _ident = self.local_path
        _invalids = []
        _imd = self.metadata
        if not self.metadata.width or not self.metadata.height:
            _invalids.append("no width/height")

        # compression
        if _imd.compression != 1:
            _invalids.append(TAG_LABELS[COMPRESSION])

        # samples per pixel
        _ssp = _imd.samples_per_pixel
        if _ssp == UNSET:
            _invalids.append(f"{TAG_LABELS[SAMPLESPERPIXEL]}: {UNSET}")
        elif _ssp not in GREYSCALE_OR_RGB:
            _invalids.append(f"{TAG_LABELS[SAMPLESPERPIXEL]}: {_ssp}")

        # resolution
        if _imd.resolution_unit != RES_UNIT_DPI:
            _invalids.append(TAG_LABELS[RESOLUTION_UNIT])
        if _imd.xRes == 0 :
            _invalids.append(f"{TAG_LABELS[X_RESOLUTION]}: 0")
        if _imd.yRes == 0 :
            _invalids.append(f"{TAG_LABELS[Y_RESOLUTION]}: 0")
        _xres: IFDRational = _imd.xRes
        if _xres.real.denominator != 1:
            _invalids.append(_imd.xRes)
        _yres: IFDRational = _imd.yRes
        if _yres.real.denominator != 1:
            _invalids.append(_imd.yRes)

        # photometric interpretation
        _pmetric = _imd.photometric_interpretation
        if _pmetric == 2 and self.profile != ADOBE_PROFILE_NAME:
            _invalids.append(_pmetric)
        elif _pmetric not in PHOTOMETRICS:
            _invalids.append(TAG_LABELS[PHOTOMETRIC_INTERPRETATION])

        # model
        if _imd.model == UNSET:
            _invalids.append(TAG_LABELS[MODEL])

        # software
        if _imd.software == UNSET:
            _invalids.append(TAG_LABELS[SOFTWARE])

        if len(_invalids) > 0:
            self.invalids = _invalids
        return _invalids
    
    def _inspect(self, tag_v2, required_tags) -> ImageMetadata:
        """Read information from TIFF Tag map
        * COMPRESSION
        """
        _image_md = ImageMetadata()
        _missing_tags = []
        for _tag in required_tags:
            if not _tag in tag_v2:
                _label = TAG_LABELS[_tag]
                _missing_tags.append(_label)
        for _tag in tag_v2:
            if _tag in TAG_LABELS:
                _label = TAG_LABELS[_tag]
                setattr(_image_md, _label, tag_v2[_tag])
        return _image_md

    def __str__(self):
        _fsize = f"{self.file_size}"
        _md = self.metadata
        _mds_res = f"({_md.xRes},{_md.yRes},{_md.resolution_unit})"
        _mds = f"\t{_mds_res}\t{_md.created}\t{_md.artist}\t{_md.copyright}\t{_md.software}\t{self.profile}"
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


def validate_tiff(tif_path, required_data=None):
    """Ensure provided image data contains
    required metadata information concerning
    resolution and alike
    """

    if not group_can_read(tif_path):
        raise FSReadException(f"Group not permitted to read {tif_path}!")
    if required_data is None:
        required_data = DEFAULT_REQUIRED_TIFF_TAGS

    _file_size = os.path.getsize(tif_path)
    if _file_size < 1024:
        msg = f"Invalid filesize {_file_size} Bytes for {tif_path}!"
        raise FSReadException(msg)
    _img_data = Image(tif_path)
    _img_data.file_size = _file_size
    _ts_object = datetime.datetime.now()
    _img_data.time_stamp = _ts_object.strftime(DATETIME_SRC_FORMAT)
    _img_data.file_size = _file_size

    try:
        # open resource
        _pil_img: TiffImageFile = PILImage.open(tif_path)
        _image_bytes = _pil_img.tobytes()
        _img_data.image_checksum = sha512(_image_bytes).hexdigest()
        _image_tags: ImageFileDirectory_v2 = _pil_img.tag_v2

        # read information from TIF TAG V2 section
        _meta_data: ImageMetadata = _img_data._inspect(_image_tags, _img_data)
        _meta_data.color_space = _pil_img.mode

        # datetime present?
        if _meta_data.created is None or _meta_data.created == UNSET:
            ctime = os.stat(tif_path).st_ctime
            dt_object = datetime.datetime.fromtimestamp(ctime)
            _meta_data.created = dt_object.strftime(DATETIME_SRC_FORMAT)
        # read profile information
        _profile = _pil_img.info.get('icc_profile')
        if _profile:
            _profile_data = io.BytesIO(_profile)
            _profile_cms = ImageCms.ImageCmsProfile(_profile_data)
            if _profile_cms:
                _profile_name = ImageCms.getProfileDescription(_profile_cms)
                if _profile_name:
                    _stripped = _profile_name.strip()
                _img_data.profile = _stripped
        _img_data.metadata = _meta_data
        _invalids = _img_data.validate()
        if len(_invalids) > 0:
            raise ImageInvalidException(_invalids)
        _pil_img.close()
    except Exception as exc:
        raise ImageInvalidException(exc.args[0])
    return _img_data

