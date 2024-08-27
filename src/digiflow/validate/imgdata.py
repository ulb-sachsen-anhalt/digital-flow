"""Image / TIFF-data specific Validation API"""

import dataclasses
import datetime
import hashlib
import io
import os
import typing

from pathlib import Path

from PIL import Image as PILImage, ImageCms
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

import digiflow.common as dfc

from .common import (
    INVALID_LABEL_RANGE,
    INVALID_LABEL_TYPE,
    INVALID_LABEL_UNSET,
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


@dataclasses.dataclass
class ImageMetadata:
    """Wrap image metadata information (i.e. EXIF)
    """

    width = 0
    height = 0
    compression = UNSET_NUMBR
    photometric_interpretation = UNSET_NUMBR
    samples_per_pixel = UNSET_NUMBR
    model = dfc.UNSET_LABEL
    xRes = UNSET_NUMBR
    yRes = UNSET_NUMBR
    resolution_unit = UNSET_NUMBR
    color_space = UNSET_NUMBR
    artist = dfc.UNSET_LABEL
    copyright = dfc.UNSET_LABEL
    software = dfc.UNSET_LABEL
    created = dfc.UNSET_LABEL  # EXIF TAG 306 DATE_TIME
    channel = UNSET_NUMBR  # EFIX TAG 258 BITSPERSAMPLE


@dataclasses.dataclass
class Image:
    """Store required data"""

    local_path: str
    url = dfc.UNSET_LABEL
    file_size = 0
    time_stamp = None
    profile = dfc.UNSET_LABEL
    image_checksum = dfc.UNSET_LABEL
    metadata: ImageMetadata = None
    invalids = []

    def read(self):
        """Try to read image data"""
        try:
            dt_object = datetime.datetime.now()
            self.time_stamp = dt_object.strftime(DATETIME_SRC_FORMAT)
            self.file_size = os.path.getsize(self.local_path)
            # open resource
            pil_img: TiffImageFile = PILImage.open(self.local_path)
            # due PIL.TiffImagePlugin problems to handle
            # tag STRIPOFFSETS properly
            image_bytes = pil_img.tobytes()
            self.image_checksum = hashlib.sha512(image_bytes).hexdigest()
            # read information from TIF TAG V2 section
            meta_data: ImageMetadata = self._read(pil_img)
            meta_data.color_space = pil_img.mode

            # datetime present?
            if meta_data.created is None or meta_data.created == dfc.UNSET_LABEL:
                ctime = os.stat(self.local_path).st_ctime
                dt_object = datetime.datetime.fromtimestamp(ctime)
                meta_data.created = dt_object.strftime(DATETIME_SRC_FORMAT)
            self.metadata = meta_data
            pil_img.close()
        except Exception as exc:
            msg = f'{self.local_path}: {exc.args[0]}'
            raise InvalidImageDataException(msg) from exc

    def _read(self, img_data: TiffImageFile) -> ImageMetadata:
        """Read present TIFF metadata information
        Please note, ICC Profile requires special
        care since this is included in bytes
        """

        image_tags: ImageFileDirectory_v2 = img_data.tag_v2
        image_md = ImageMetadata()
        for a_tag in image_tags:
            if a_tag in TIFF_METADATA_LABELS:
                a_label = TIFF_METADATA_LABELS[a_tag]
                if a_tag != ICCPROFILE:
                    tag_val = image_tags[a_tag]
                    if isinstance(tag_val, str):
                        tag_val = str(tag_val).strip()
                    setattr(image_md, a_label, tag_val)
                else:
                    icc_profile = img_data.info.get('icc_profile')
                    if icc_profile:
                        profile_bytes = io.BytesIO(icc_profile)
                        profile_cms = ImageCms.ImageCmsProfile(profile_bytes)
                        if profile_cms:
                            profile_name = ImageCms.getProfileDescription(profile_cms)
                            if profile_name:
                                name_stripped = profile_name.strip()
                            self.profile = name_stripped
        return image_md

    def __str__(self):
        img_fsize = f"{self.file_size}"
        img_md = self.metadata
        mds_res = f"({img_md.xRes},{img_md.yRes},{img_md.resolution_unit})"
        img_mds = f"\t{mds_res}\t{img_md.created}\t{img_md.artist}\t{img_md.copyright}\t{img_md.model}\t{img_md.software}\t{self.profile}"
        invalids = f"INVALID{self.invalids}" if len(self.invalids) > 0 else 'VALID'
        return f"{self.url}\t{self.image_checksum}\t{img_fsize}\t{self.metadata.width}x{self.metadata.height}\t{img_mds}\t{self.time_stamp}\t{invalids}"


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
        f_size = os.path.getsize(self.input_data)
        if f_size < MIN_SCAN_FILESIZE:
            msg = f"{INVALID_LABEL_RANGE} filesize {f_size}b < {MIN_SCAN_FILESIZE}b"
            self.invalids.append(Invalid(self.label, self.input_data, msg))
        input_image: Image = Image(self.input_data)
        input_image.read()
        img_md: ImageMetadata = input_image.metadata
        if not img_md.width or not img_md.height:
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
        input_image: Image = self.input_data
        img_md: ImageMetadata = input_image.metadata
        md_spp = img_md.samples_per_pixel
        if md_spp == dfc.UNSET_LABEL:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_UNSET} {LABEL_SAMPLES_PIXEL}"))
        elif md_spp not in GREYSCALE_OR_RGB:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_SAMPLES_PIXEL} {md_spp} not in {GREYSCALE_OR_RGB}"))
        if not all(c <= MAX_CHANNEL_DEPTH for c in img_md.channel):
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_CHANNEL} {img_md.channel} > {MAX_CHANNEL_DEPTH}"))
        return super().valid()


class ScanValidatorCompression(Validator):
    """Validate EXIF metadata uncompressed set"""

    def __init__(self, input_data: Image):
        super().__init__(LABEL_SCAN_VALIDATOR_COMPRESSION, input_data)

    def valid(self) -> bool:
        """Check flag umcompressed data set"""

        input_image: Image = self.input_data
        img_md: ImageMetadata = input_image.metadata
        if img_md.compression != 1:
            msg = f"{INVALID_LABEL_RANGE} {LABEL_COMPRESSION} {img_md.compression} != 1"
            self.invalids.append(Invalid(self.label, input_image.local_path, msg))
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
        input_image: Image = self.input_data
        img_loc = input_image.local_path
        img_md: ImageMetadata = input_image.metadata
        a_prefix = self.label
        if img_md.resolution_unit == UNSET_NUMBR:
            inv01 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_UNIT}")
            self.invalids.append(inv01)
        elif img_md.resolution_unit != RES_UNIT_DPI:
            inv02 = Invalid(
                a_prefix, img_loc, f"{INVALID_LABEL_RANGE} {LABEL_RES_UNIT} {img_md.resolution_unit} != {RES_UNIT_DPI}")
            self.invalids.append(inv02)
        if img_md.xRes == UNSET_NUMBR:
            inv03 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_X}")
            self.invalids.append(inv03)
        else:
            if img_md.xRes < MIN_SCAN_RESOLUTION:
                inv04 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_RANGE}{LABEL_RES_X}: {img_md.xRes}")
                self.invalids.append(inv04)
            x_res: IFDRational = img_md.xRes
            if x_res.real.denominator != 1:
                inv07 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_TYPE} {LABEL_RES_X}: {img_md.xRes}")
                self.invalids.append(inv07)
        if img_md.yRes == UNSET_NUMBR:
            inv05 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_Y}")
            self.invalids.append(inv05)
        else:
            if img_md.yRes < MIN_SCAN_RESOLUTION:
                inv06 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_RANGE} {LABEL_RES_Y}: {img_md.yRes}")
                self.invalids.append(inv06)
            y_res: IFDRational = img_md.yRes
            if y_res.real.denominator != 1:
                inv08 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_TYPE} {LABEL_RES_Y}: {img_md.yRes}")
                self.invalids.append(inv08)
        return super().valid()


class ScanValidatorPhotometric(Validator):
    """Check EXIF Photometric information
    * Photometric interpretation must be Grayscale or RGB 
    * if RGB, then profile must be ADOBE RGB 1998
    """

    def __init__(self, input_data: Image):
        super().__init__(LABEL_SCAN_VALIDATOR_PHOTOMETRICS, input_data)

    def valid(self) -> bool:
        input_image: Image = self.input_data
        img_md: ImageMetadata = input_image.metadata
        pmetrics = img_md.photometric_interpretation
        if pmetrics not in PHOTOMETRICS:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_GRAY_RGB} {pmetrics} not in {PHOTOMETRICS}"))
        if pmetrics == 2 and input_image.profile != ADOBE_PROFILE_NAME:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_PROFILE} != {ADOBE_PROFILE_NAME}"))
        return super().valid()


class ValidatorFactory:
    """Encapsulate access to actual validator objects"""

    validators: typing.Dict = {
        LABEL_SCAN_VALIDATOR_FILEDATA: ScanValidatorFile,
        LABEL_SCAN_VALIDATOR_CHANNEL: ScanValidatorChannel,
        LABEL_SCAN_VALIDATOR_COMPRESSION: ScanValidatorCompression,
        LABEL_SCAN_VALIDATOR_RESOLUTION: ScanValidatorResolution,
        LABEL_SCAN_VALIDATOR_PHOTOMETRICS: ScanValidatorPhotometric,
    }

    @staticmethod
    def register(validator_label:str, validator_class: Validator):
        """Extend given validators at runtime"""
        ValidatorFactory.validators[validator_label] = validator_class

    @staticmethod
    def unregister(validator_label:str):
        """Manage validators at runtime"""
        if validator_label in ValidatorFactory.validators:
            del ValidatorFactory.validators[validator_label]

    @staticmethod
    def get(validator_label: str) -> Validator:
        """Get Validator object for label"""

        if validator_label not in ValidatorFactory.validators:
            raise NotImplementedError(f"No implementation for {validator_label}!")
        return ValidatorFactory.validators[validator_label]


class ScanValidatorCombined(Validator):
    """Encapsulate image validation"""

    validator_factory: ValidatorFactory = ValidatorFactory

    def __init__(self, path_input: Path, validator_labels: typing.List[str]):
        super().__init__(LABEL_SCAN_VALIDATOR_COMBINED, path_input)
        self.path_input = path_input
        self.validator_labels: typing.List[str] = validator_labels
        self.img_data: Image = None

    @staticmethod
    def register(validator_label, validator_class):
        """Extend validators at runtime"""
        if validator_label not in ScanValidatorCombined.validator_factory.validators:
            ScanValidatorCombined.validator_factory.register(validator_label, validator_class)

    def valid(self) -> bool:
        self.img_data = Image(self.input_data)
        self.img_data.url = self.path_input
        self.img_data.read()
        for a_label in self.validator_labels:
            clazz = ScanValidatorCombined.validator_factory.get(a_label)
            validator: Validator = clazz(self.img_data)
            if not validator.valid():
                self.invalids.extend(validator.invalids)
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
    image_val = ScanValidatorCombined(tif_path, required_validators)
    image_val.valid()
    return image_val
