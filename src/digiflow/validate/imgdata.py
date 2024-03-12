"""Image / TIFF-data specific Validation API"""

import datetime
import io
import os

from dataclasses import (
    dataclass,
)
from hashlib import (
    sha512
)
from pathlib import (
    Path,
)
from typing import (
    Dict,
    List,
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

from .common import (
    INVALID_LABEL_RANGE,
    INVALID_LABEL_TYPE,
    INVALID_LABEL_UNSET,
    UNSET_LABEL,
    UNSET_NUMBR,
    Validator,
    Invalid,
)
from .fsdata import (
    FSReadException,
    group_can_read,
)

# special, additional tag
MODEL = 272  # EXIF 272

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
LABEL_WIDTH = TIFF_METADATA_LABELS[IMAGEWIDTH]
LABEL_HEIGHT = TIFF_METADATA_LABELS[IMAGELENGTH]
LABEL_RES_UNIT = TIFF_METADATA_LABELS[RESOLUTION_UNIT]
LABEL_RES_X = TIFF_METADATA_LABELS[X_RESOLUTION]
LABEL_RES_Y = TIFF_METADATA_LABELS[Y_RESOLUTION]
LABEL_CHANNEL = TIFF_METADATA_LABELS[BITSPERSAMPLE]
LABEL_COMPRESSION = TIFF_METADATA_LABELS[COMPRESSION]
LABEL_SAMPLES_PIXEL = TIFF_METADATA_LABELS[SAMPLESPERPIXEL]
LABEL_GRAY_RGB = TIFF_METADATA_LABELS[PHOTOMETRIC_INTERPRETATION]
LABEL_PROFILE = TIFF_METADATA_LABELS[ICCPROFILE]

# module constants
MIN_SCAN_FILESIZE = 1024    # Bytes
MIN_SCAN_RESOLUTION = 300   # like 300 DPI
RES_UNIT_DPI = 2            # EXIF Value for DPI
MAX_CHANNEL_DEPTH = 8       # 8 bits / Channel
DEFAULT_COMPRESSION = 1     # EXIF 259 1=uncompressed
PHOTOMETRICS = [1, 2]       # EXIF 262 1 = BlackIsZero, 2 = rgb
GREYSCALE_OR_RGB = [1, 3]   # EXIF 277 1 = greyscale, 3 = 3-channel RGB
ADOBE_PROFILE_NAME = 'Adobe RGB (1998)'  # Preferred ICC-Profile for RGB-TIF

# date formatting
DATETIME_MIX_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATETIME_SRC_FORMAT = "%Y:%m:%d %H:%M:%S"

# labels for clazzes
LABEL_SCAN_VALIDATOR_COMBINED = 'ScanValidatorCombined'
LABEL_SCAN_VALIDATOR_CHANNEL = 'ScanValidatorChannel'
LABEL_SCAN_VALIDATOR_COMPRESSION = 'ScanValidatorCompression'
LABEL_SCAN_VALIDATOR_FILEDATA = 'ScanValidatorFiledata'
LABEL_SCAN_VALIDATOR_PHOTOMETRICS = 'ScanValidatorPhotometrics'
LABEL_SCAN_VALIDATOR_RESOLUTION = 'ScanValidatorResolution'


class InvalidImageDataException(Exception):
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
            self.file_size = os.path.getsize(self.local_path)
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
            raise InvalidImageDataException(exc.args[0])

    def _read(self, img_data: TiffImageFile) -> ImageMetadata:
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

    def __str__(self):
        _fsize = f"{self.file_size}"
        _md = self.metadata
        _mds_res = f"({_md.xRes},{_md.yRes},{_md.resolution_unit})"
        _mds = f"\t{_mds_res}\t{_md.created}\t{_md.artist}\t{_md.copyright}\t{_md.model}\t{_md.software}\t{self.profile}"
        _invalids = f"INVALID{self.invalids}" if len(
            self.invalids) > 0 else 'VALID'
        return f"{self.url}\t{self.image_checksum}\t{_fsize}\t{self.metadata.width}x{self.metadata.height}\t{_mds}\t{self.time_stamp}\t{_invalids}"


class ScanValidatorFile(Validator):
    """Validate file on very basic level, i.e.
    whether it can be read, file size suspicous
    or no metadata tags for width/height present
    """

    def __init__(self, input_data: Path):
        super().__init__(LABEL_SCAN_VALIDATOR_FILEDATA, input_data)
        if isinstance(self.input_data, Image):
            self.input_data = self.input_data.local_path

    def valid(self) -> bool:
        """Fail faster: If file can't even be read by group,
        """

        if not group_can_read(self.input_data):
            raise FSReadException(f'No permission to read {self.input_data}!')
        _file_size = os.path.getsize(self.input_data)
        if _file_size < MIN_SCAN_FILESIZE:
            _msg = f"{INVALID_LABEL_RANGE} filesize {_file_size}b < {MIN_SCAN_FILESIZE}b"
            self.invalids.append(Invalid(self.label, self.input_data, _msg))
        _input: Image = Image(self.input_data)
        _input.read()
        _imd: ImageMetadata = _input.metadata
        if not _imd.width or not _imd.height:
            self.invalids.append(Invalid(self.label, self.input_data,
                                 f"{INVALID_LABEL_UNSET} width/height"))
        return super().valid()


class ScanValidatorChannel(Validator):
    """Validate EXIF metadata concerning
    maximal processable channel depth and
    number of channels
    """

    def __init__(self, input_data: Image):
        super().__init__(LABEL_SCAN_VALIDATOR_CHANNEL, input_data)

    def valid(self) -> bool:
        _input: Image = self.input_data
        _imd: ImageMetadata = _input.metadata
        _spp = _imd.samples_per_pixel
        if _spp == UNSET_LABEL:
            self.invalids.append(Invalid(self.label, _input.local_path,
                                 f"{INVALID_LABEL_UNSET} {LABEL_SAMPLES_PIXEL}"))
        elif _spp not in GREYSCALE_OR_RGB:
            self.invalids.append(Invalid(self.label, _input.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_SAMPLES_PIXEL} {_spp} not in {GREYSCALE_OR_RGB}"))
        if not all(c <= MAX_CHANNEL_DEPTH for c in _imd.channel):
            self.invalids.append(Invalid(self.label, _input.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_CHANNEL} {_imd.channel} > {MAX_CHANNEL_DEPTH}"))
        return super().valid()


class ScanValidatorCompression(Validator):
    """Validate EXIF metadata uncompressed set"""

    def __init__(self, input_data: Image):
        super().__init__(LABEL_SCAN_VALIDATOR_COMPRESSION, input_data)

    def valid(self) -> bool:
        """Check flag umcompressed data set"""

        _input: Image = self.input_data
        _imd: ImageMetadata = _input.metadata
        if _imd.compression != 1:
            _info = f"{INVALID_LABEL_RANGE} {LABEL_COMPRESSION} {_imd.compression} != 1"
            self.invalids.append(Invalid(self.label, _input.local_path, _info))
        return super().valid()


class ScanValidatorResolution(Validator):
    """Validate EXIF metadata concerning
    values of required Resolution information
    * XResolution / YResolution: must not be rationals
    * Resolution Unit must not be different from DPI
    """

    def __init__(self, input_data: Image):
        super().__init__(LABEL_SCAN_VALIDATOR_RESOLUTION, input_data)

    def valid(self) -> bool:
        _input: Image = self.input_data
        _loc = _input.local_path
        _imd: ImageMetadata = _input.metadata
        _prefix = self.label
        if _imd.resolution_unit == UNSET_NUMBR:
            _inv01 = Invalid(_prefix, _loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_UNIT}")
            self.invalids.append(_inv01)
        elif _imd.resolution_unit != RES_UNIT_DPI:
            _inv02 = Invalid(
                _prefix, _loc, f"{INVALID_LABEL_RANGE} {LABEL_RES_UNIT} {_imd.resolution_unit} != {RES_UNIT_DPI}")
            self.invalids.append(_inv02)
        if _imd.xRes == UNSET_NUMBR:
            _inv03 = Invalid(_prefix, _loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_X}")
            self.invalids.append(_inv03)
        else:
            if _imd.xRes < MIN_SCAN_RESOLUTION:
                _inv04 = Invalid(_prefix, _loc, f"{INVALID_LABEL_RANGE}{LABEL_RES_X}: {_imd.xRes}")
                self.invalids.append(_inv04)
            _xres: IFDRational = _imd.xRes
            if _xres.real.denominator != 1:
                _inv07 = Invalid(_prefix, _loc, f"{INVALID_LABEL_TYPE} {LABEL_RES_X}: {_imd.xRes}")
                self.invalids.append(_inv07)
        if _imd.yRes == UNSET_NUMBR:
            _inv05 = Invalid(_prefix, _loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_Y}")
            self.invalids.append(_inv05)
        else:
            if _imd.yRes < MIN_SCAN_RESOLUTION:
                _inv06 = Invalid(_prefix, _loc, f"{INVALID_LABEL_RANGE} {LABEL_RES_Y}: {_imd.yRes}")
                self.invalids.append(_inv06)
            _yres: IFDRational = _imd.yRes
            if _yres.real.denominator != 1:
                _inv08 = Invalid(_prefix, _loc, f"{INVALID_LABEL_TYPE} {LABEL_RES_Y}: {_imd.yRes}")
                self.invalids.append(_inv08)
        return super().valid()


class ScanValidatorPhotometric(Validator):
    """Check EXIF Photometric information
    * Photometric interpretation must be Grayscale or RGB 
    * if RGB, then profile must be ADOBE RGB 1998
    """

    def __init__(self, input_data: Image):
        super().__init__(LABEL_SCAN_VALIDATOR_PHOTOMETRICS, input_data)

    def valid(self) -> bool:
        _input: Image = self.input_data
        _imd: ImageMetadata = _input.metadata
        _pmetric = _imd.photometric_interpretation
        if _pmetric not in PHOTOMETRICS:
            self.invalids.append(Invalid(self.label, _input.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_GRAY_RGB} {_pmetric} not in {PHOTOMETRICS}"))
        if _pmetric == 2 and _input.profile != ADOBE_PROFILE_NAME:
            self.invalids.append(Invalid(self.label, _input.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_PROFILE} != {ADOBE_PROFILE_NAME}"))
        return super().valid()


class ValidatorFactory:

    validators: Dict = {
        LABEL_SCAN_VALIDATOR_FILEDATA: ScanValidatorFile,
        LABEL_SCAN_VALIDATOR_CHANNEL: ScanValidatorChannel,
        LABEL_SCAN_VALIDATOR_COMPRESSION: ScanValidatorCompression,
        LABEL_SCAN_VALIDATOR_RESOLUTION: ScanValidatorResolution,
        LABEL_SCAN_VALIDATOR_PHOTOMETRICS: ScanValidatorPhotometric, 
    }

    @staticmethod
    def register(validator_label:str, validator_class: Validator):
        ValidatorFactory.validators[validator_label] = validator_class

    @staticmethod
    def unregister(validator_label:str):
        if validator_label in ValidatorFactory.validators:
            del ValidatorFactory.validators[validator_label]

    @staticmethod
    def get(validator_label: str) -> Validator:
        """Get Validator object for label"""

        if validator_label not in ValidatorFactory.validators:
            raise Exception(f"Missing Implementation for {validator_label}!")
        return ValidatorFactory.validators[validator_label]


class ScanValidatorCombined(Validator):
    """Encapsulate image validation"""

    validator_factory: ValidatorFactory = ValidatorFactory

    def __init__(self, path_input: Path, validator_labels: List[str]):
        super().__init__(LABEL_SCAN_VALIDATOR_COMBINED, path_input)
        self.path_input = path_input
        self.validator_labels: List[str] = validator_labels
        self.img_data: Image = None

    @staticmethod
    def register(validator_label, validator_class):
        if validator_label not in ScanValidatorCombined.validator_factory.validators:
            ScanValidatorCombined.validator_factory.register(validator_label, validator_class)

    def valid(self) -> bool:
        self.img_data = Image(self.input_data)
        self.img_data.url = self.path_input
        self.img_data.read()
        for _label in self.validator_labels:
            _val_clazz = ScanValidatorCombined.validator_factory.get(_label)
            _val: Validator = _val_clazz(self.img_data)
            if not _val.valid():
                self.invalids.extend(_val.invalids)
        return super().valid()


def validate_tiff(tif_path, required_validators=None):
    """Ensure provided image data contains
    required metadata information concerning
    resolution and alike

    High-level validation API to trigger
    comma-separated labelled Validators
    """

    if required_validators is None:
        required_validators = ValidatorFactory.validators
    _image_val = ScanValidatorCombined(tif_path, required_validators)
    _image_val.valid()
    return _image_val
