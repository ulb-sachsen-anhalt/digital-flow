"""Image / TIFF-data specific Validation API"""

import dataclasses
import datetime
import hashlib
import inspect
import io
import os
import typing

from pathlib import Path

from PIL import Image as PILImage, ImageCms
from PIL.TiffImagePlugin import (
    IMAGEWIDTH,  # 256
    IMAGELENGTH,  # 257
    BITSPERSAMPLE,  # 258 (8,8,8)=8 Bit_depth per channel
    COMPRESSION,  # 259 1=uncompressed
    PHOTOMETRIC_INTERPRETATION,  # 262 1=BlackIsZero, 2=RGB
    SAMPLESPERPIXEL,  # 277 1=greyscale, 3=3-channel RGB
    X_RESOLUTION,  # 282
    Y_RESOLUTION,  # 283
    RESOLUTION_UNIT,  # 296 2=Inches,3=cm
    SOFTWARE,  # 305
    DATE_TIME,  # 306
    ARTIST,  # 315
    COPYRIGHT,  # 33423
    ICCPROFILE,  # 34675
    ImageFileDirectory_v2,
    TiffImageFile,
    IFDRational,
)

import digiflow.common as dfc
import digiflow.validate.common as dfvc
import digiflow.validate.fsdata as dfvfs

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
    IMAGEWIDTH: "width",
    IMAGELENGTH: "height",
    BITSPERSAMPLE: "channel",
    X_RESOLUTION: "resolution_x",
    Y_RESOLUTION: "resolution_y",
    RESOLUTION_UNIT: "resolution_unit",
    ARTIST: "artist",
    COMPRESSION: "compression",
    COPYRIGHT: "copyright",
    MODEL: "model",
    SOFTWARE: "software",
    DATE_TIME: "created",
    SAMPLESPERPIXEL: "samples_per_pixel",
    PHOTOMETRIC_INTERPRETATION: "photometric_interpretation",
    ICCPROFILE: "icc_profile",
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
MIN_SCAN_FILESIZE = 1024  # Bytes
RES_300 = 300  # like 300 DPI
RES_UNIT_DPI = 2  # EXIF Value for DPI
MAX_CHANNEL_DEPTH = 8  # 8 bits / Channel
DEFAULT_COMPRESSION = 1  # EXIF 259 1=uncompressed
PHOTOMETRICS = [1, 2]  # EXIF 262 1 = BlackIsZero, 2 = rgb
GREYSCALE_OR_RGB = [1, 3]  # EXIF 277 1 = greyscale, 3 = 3-channel RGB
ADOBE_PROFILE_NAME = "Adobe RGB (1998)"  # Preferred ICC-Profile for RGB-TIF

# date formatting
DATETIME_MIX_FORMAT = "%Y-%m-%dT%H:%M:%S"

# labels for clazzes
LABEL_SCAN_VALIDATOR_COMBINED = "ScanValidatorCombined"
LABEL_SCAN_VALIDATOR_CHANNEL = "ScanValidatorChannel"
LABEL_SCAN_VALIDATOR_COMPRESSION = "ScanValidatorCompression"
LABEL_SCAN_VALIDATOR_FILE = "ScanValidatorFile"
LABEL_SCAN_VALIDATOR_PHOTOMETRICS = "ScanValidatorPhotometrics"
LABEL_SCAN_VALIDATOR_RESOLUTION = "ScanValidatorResolution"

COMMON_SCAN_VALIDATOR_LABELS = [
    LABEL_SCAN_VALIDATOR_FILE,
    LABEL_SCAN_VALIDATOR_CHANNEL,
    LABEL_SCAN_VALIDATOR_COMPRESSION,
    LABEL_SCAN_VALIDATOR_RESOLUTION,
    LABEL_SCAN_VALIDATOR_PHOTOMETRICS,
]


class InvalidImageDataException(Exception):
    """Mark invalid image data"""


@dataclasses.dataclass
class ScanValidatorConfig:
    """Centralized configuration for image validators

    This dataclass bundles all validation parameters together,
    providing type safety, documentation, and easy serialization.
    """

    # File validator
    valid_min_size: int = MIN_SCAN_FILESIZE

    # Channel validator
    valid_channels: typing.List[int] = dataclasses.field(
        default_factory=GREYSCALE_OR_RGB.copy
    )
    max_channel_depth: int = MAX_CHANNEL_DEPTH

    # Compression validator
    valid_compression: int = DEFAULT_COMPRESSION

    # Resolution validator
    valid_resolutions: typing.List[int] = dataclasses.field(
        default_factory=lambda: [RES_300]
    )
    valid_resolution_unit: int = RES_UNIT_DPI

    # Photometric validator
    valid_photometrics: typing.List[int] = dataclasses.field(
        default_factory=PHOTOMETRICS.copy
    )
    required_rgb_profile: str = ADOBE_PROFILE_NAME

    def to_dict(self) -> dict:
        """Convert configuration to dictionary for kwargs

        Returns:
            Dictionary representation of configuration
        """
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, config_dict: dict) -> "ScanValidatorConfig":
        """Create ValidatorConfig from dictionary

        Args:
            config_dict: Dictionary with configuration values

        Returns:
            New ValidatorConfig instance
        """
        return cls(**config_dict)


@dataclasses.dataclass
class ImageMetadata:
    """Wrap image metadata information (i.e. EXIF)"""

    width = 0
    height = 0
    compression = dfvc.UNSET_NUMBR
    photometric_interpretation = dfvc.UNSET_NUMBR
    samples_per_pixel = dfvc.UNSET_NUMBR
    model = dfc.UNSET_LABEL
    resolution_x: typing.Optional[IFDRational] = None
    resolution_y: typing.Optional[IFDRational] = None
    resolution_unit = dfvc.UNSET_NUMBR
    artist = dfc.UNSET_LABEL
    copyright = dfc.UNSET_LABEL
    software = dfc.UNSET_LABEL
    created = dfc.UNSET_LABEL  # EXIF TAG 306 DATE_TIME
    channel = (dfvc.UNSET_NUMBR,)  # EFIX TAG 258 BITSPERSAMPLE


class InputImage(dfvc.InputFile):
    """Store required data"""

    def __init__(self, local_path):
        super().__init__(local_path)
        self.profile = dfc.UNSET_LABEL
        self.metadata: typing.Optional[ImageMetadata] = None

    def read(self):
        """Try to read image data"""
        try:
            self.file_size = os.path.getsize(self.input_path)
            hash_val = hashlib.sha512()
            with open(self.input_path, "rb") as freader:
                hash_val.update(freader.read())
            self.check_sum_512 = hash_val.hexdigest()
            # read information from TIF TAG V2 section
            open_image = PILImage.open(self.input_path)
            pil_img: typing.Optional[TiffImageFile] = None
            if isinstance(open_image, TiffImageFile):
                pil_img = open_image
            else:
                msg = f"Not a valid TIFF image: {self.input_path}"
                raise InvalidImageDataException(msg)
            meta_data: ImageMetadata = self._read(pil_img)
            # datetime present?
            if meta_data.created is None or meta_data.created == dfc.UNSET_LABEL:
                ctime = os.stat(self.input_path).st_ctime
                dt_object = datetime.datetime.fromtimestamp(ctime)
                meta_data.created = dt_object.strftime(dfvc.DATETIME_SRC_FORMAT)
            self.metadata = meta_data
            pil_img.close()
        except Exception as exc:
            msg = f"{self.input_path}: {exc.args[0]}"
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
                    icc_profile = img_data.info.get("icc_profile")
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
        assert img_md is not None
        mds_res = (
            f"({img_md.resolution_x},{img_md.resolution_y},{img_md.resolution_unit})"
        )
        img_mds = f"\t{mds_res}\t{img_md.created}\t{img_md.artist}\t{img_md.copyright}\t{img_md.model}\t{img_md.software}\t{self.profile}"
        return f"{self.input_path}\t{img_fsize}\t{self.check_sum_512}\t{img_md.width}x{img_md.height}\t{img_mds}\t{self.time_stamp}"


class ScanValidator(dfvc.Validator):
    """Encapsulate local image scan data validation"""

    def __init__(self, input_data, **kwargs):
        super().__init__(input_data, **kwargs)
        self.set_data(input_data)

    def set_data(self, input_data) -> None:
        if isinstance(input_data, Path):
            self.input_path = input_data
            input_image: InputImage = InputImage(input_data)
            input_image.read()
            self.input_file = input_image
        elif isinstance(input_data, InputImage):
            self.input_file = input_data
            self.input_path = input_data.input_path

    def check(self) -> None:
        """Run all validators and collect invalids"""

        if self.input_file is None:
            raise InvalidImageDataException("No image metadata available for validation!")
        if self.input_file.input_path is None:
            raise dfvfs.FSReadException("No input path provided for file validation!")


class ScanValidatorFileStats(ScanValidator):
    """Validate file on very basic level, i.e.
    whether it can be read, file size suspicous
    or no metadata tags for width/height present
    """

    def __init__(self, input_data, **kwargs):
        super().__init__(input_data, **kwargs)
        self.min_size = kwargs.get("valid_min_size", MIN_SCAN_FILESIZE)

    @property
    def label(self) -> str:
        return LABEL_SCAN_VALIDATOR_FILE

    def check(self) -> None:
        """Fail faster: If file can't even be read by group,"""

        super().check()
        if not dfvfs.group_can_read(self.input_path):
            raise dfvfs.FSReadException(f"No permission to read {self.input_path}!")
        assert self.input_path is not None
        f_size = os.path.getsize(self.input_path)
        if f_size < self.min_size:
            msg = f"{dfvc.INVALID_LABEL_RANGE} filesize {f_size}b < {self.min_size}b"
            self.invalids.append(dfvc.Invalid(self.label, self.input_path, msg))
        assert self.input_file is not None
        assert isinstance(self.input_file, InputImage)
        assert self.input_file.metadata is not None
        img_md: ImageMetadata = self.input_file.metadata
        if not img_md.width or not img_md.height:
            self.invalids.append(
                dfvc.Invalid(
                    self.label,
                    self.input_path,
                    f"{dfvc.INVALID_LABEL_UNSET} width/height",
                )
            )


class ScanValidatorChannel(ScanValidator):
    """Validate EXIF metadata concerning
    maximal processable channel depth and
    number of channels
    """

    def __init__(self, input_data: InputImage, **kwargs):
        super().__init__(input_data)
        self.valid_channels = kwargs.get("valid_channels", GREYSCALE_OR_RGB)

    @property
    def label(self) -> str:
        return LABEL_SCAN_VALIDATOR_CHANNEL

    def check(self) -> None:
        super().check()
        assert self.input_file is not None
        assert isinstance(self.input_file, InputImage)
        assert self.input_file.metadata is not None
        img_md: ImageMetadata = self.input_file.metadata
        assert img_md is not None
        assert self.input_path is not None
        md_spp = img_md.samples_per_pixel
        if md_spp == dfc.UNSET_LABEL:
            self.invalids.append(
                dfvc.Invalid(
                    self.label,
                    self.input_path,
                    f"{dfvc.INVALID_LABEL_UNSET} {LABEL_SAMPLES_PIXEL}",
                )
            )
        elif md_spp not in self.valid_channels:
            self.invalids.append(
                dfvc.Invalid(
                    self.label,
                    self.input_path,
                    f"{dfvc.INVALID_LABEL_RANGE} {LABEL_SAMPLES_PIXEL} {md_spp} not in {self.valid_channels}",
                )
            )
        if not all(c <= MAX_CHANNEL_DEPTH for c in img_md.channel):
            self.invalids.append(
                dfvc.Invalid(
                    self.label,
                    self.input_path,
                    f"{dfvc.INVALID_LABEL_RANGE} {LABEL_CHANNEL} {img_md.channel} > {MAX_CHANNEL_DEPTH}",
                )
            )


class ScanValidatorCompression(ScanValidator):
    """Validate EXIF metadata uncompressed set"""

    def __init__(self, input_data: InputImage, **kwargs):
        super().__init__(input_data)

    @property
    def label(self) -> str:
        return LABEL_SCAN_VALIDATOR_COMPRESSION

    def check(self) -> None:
        """Check flag umcompressed data set"""

        super().check()
        assert self.input_file is not None
        assert isinstance(self.input_file, InputImage)
        assert self.input_file.metadata is not None
        img_md: ImageMetadata = self.input_file.metadata
        if img_md.compression != 1:
            msg = f"{dfvc.INVALID_LABEL_RANGE} {LABEL_COMPRESSION} {img_md.compression} != 1"
            self.invalids.append(dfvc.Invalid(self.label, self.input_file.input_path, msg))


class ScanValidatorResolution(ScanValidator):
    """Validate EXIF metadata concerning
    values of required Resolution information
    * XResolution / YResolution: must not be rationals
    * Resolution Unit must not be different from DPI
    """

    def __init__(self, input_data: InputImage, **kwargs):
        super().__init__(input_data, **kwargs)
        self.valid_resolutions = kwargs.get("valid_resolutions", [RES_300])

    @property
    def label(self) -> str:
        return LABEL_SCAN_VALIDATOR_RESOLUTION

    def check(self) -> None:
        super().check()
        assert self.input_file is not None
        assert isinstance(self.input_file, InputImage)
        input_image: InputImage = self.input_file
        img_loc = input_image.input_path
        assert self.input_file.metadata is not None
        img_md: ImageMetadata = self.input_file.metadata
        a_prefix = self.label
        if img_md.resolution_unit == dfvc.UNSET_NUMBR:
            inv01 = dfvc.Invalid(
                a_prefix, img_loc, f"{dfvc.INVALID_LABEL_UNSET} {LABEL_RES_UNIT}"
            )
            self.invalids.append(inv01)
        elif img_md.resolution_unit != RES_UNIT_DPI:
            inv02 = dfvc.Invalid(
                a_prefix,
                img_loc,
                f"{dfvc.INVALID_LABEL_RANGE} {LABEL_RES_UNIT} {img_md.resolution_unit} != {RES_UNIT_DPI}",
            )
            self.invalids.append(inv02)
        if img_md.resolution_x == dfvc.UNSET_NUMBR:
            inv03 = dfvc.Invalid(
                a_prefix, img_loc, f"{dfvc.INVALID_LABEL_UNSET} {LABEL_RES_X}"
            )
            self.invalids.append(inv03)
        else:
            if img_md.resolution_x not in self.valid_resolutions:
                inv04 = dfvc.Invalid(
                    a_prefix,
                    img_loc,
                    f"{dfvc.INVALID_LABEL_RANGE} {LABEL_RES_X}: {img_md.resolution_x}",
                )
                self.invalids.append(inv04)
            if img_md.resolution_x is not None and isinstance(
                img_md.resolution_x, IFDRational
            ):
                x_res: IFDRational = img_md.resolution_x
                if x_res.denominator != 1:
                    inv07 = dfvc.Invalid(
                        a_prefix,
                        img_loc,
                        f"{dfvc.INVALID_LABEL_TYPE} {LABEL_RES_X}: {img_md.resolution_x}",
                    )
                    self.invalids.append(inv07)
        if img_md.resolution_y == dfvc.UNSET_NUMBR:
            inv05 = dfvc.Invalid(
                a_prefix, img_loc, f"{dfvc.INVALID_LABEL_UNSET} {LABEL_RES_Y}"
            )
            self.invalids.append(inv05)
        else:
            if img_md.resolution_y not in self.valid_resolutions:
                inv06 = dfvc.Invalid(
                    a_prefix,
                    img_loc,
                    f"{dfvc.INVALID_LABEL_RANGE} {LABEL_RES_Y}: {img_md.resolution_y}",
                )
                self.invalids.append(inv06)
            if img_md.resolution_y is not None and isinstance(
                img_md.resolution_y, IFDRational
            ):
                y_res: IFDRational = img_md.resolution_y
                if y_res.denominator != 1:
                    inv08 = dfvc.Invalid(
                        a_prefix,
                        img_loc,
                        f"{dfvc.INVALID_LABEL_TYPE} {LABEL_RES_Y}: {img_md.resolution_y}",
                    )
                    self.invalids.append(inv08)


class ScanValidatorPhotometric(ScanValidator):
    """Check EXIF Photometric information
    * Photometric interpretation must be Grayscale or RGB
    * if RGB, then profile must be ADOBE RGB 1998
    """

    def __init__(self, input_data: InputImage, **kwargs):
        super().__init__(input_data)
        self.valid_photometrics = kwargs.get("valid_photometrics", PHOTOMETRICS)

    @property
    def label(self) -> str:
        return LABEL_SCAN_VALIDATOR_PHOTOMETRICS

    def check(self) -> None:
        super().check()
        assert isinstance(self.input_file, InputImage)
        assert self.input_file.input_path is not None
        assert self.input_file.metadata is not None
        img_md: ImageMetadata = self.input_file.metadata
        pmetrics = img_md.photometric_interpretation
        if pmetrics not in self.valid_photometrics:
            self.invalids.append(
                dfvc.Invalid(
                    self.label,
                    self.input_file.input_path,
                    f"{dfvc.INVALID_LABEL_RANGE} {LABEL_GRAY_RGB} {pmetrics} not in {self.valid_photometrics}",
                )
            )
        if pmetrics == 2 and self.input_file.profile != ADOBE_PROFILE_NAME:
            self.invalids.append(
                dfvc.Invalid(
                    self.label,
                    self.input_file.input_path,
                    f"{dfvc.INVALID_LABEL_RANGE} {LABEL_PROFILE} != {ADOBE_PROFILE_NAME}",
                )
            )


class ScanValidatorFactory(dfvc.ValidatorFactory):
    """Factory for creating and managing scan validator instances

    Supports configuration management and runtime registration of validators.
    """

    _registry: typing.ClassVar[typing.Dict[str, typing.Type[dfvc.Validator]]] = {
        LABEL_SCAN_VALIDATOR_FILE: ScanValidatorFileStats,
        LABEL_SCAN_VALIDATOR_CHANNEL: ScanValidatorChannel,
        LABEL_SCAN_VALIDATOR_COMPRESSION: ScanValidatorCompression,
        LABEL_SCAN_VALIDATOR_RESOLUTION: ScanValidatorResolution,
        LABEL_SCAN_VALIDATOR_PHOTOMETRICS: ScanValidatorPhotometric,
    }

    def __init__(self, config: typing.Optional[ScanValidatorConfig] = None):
        """Initialize factory with configuration

        Args:
            config: Configuration to use. Creates default if None.
        """
        super().__init__(config)
        self.config = config or ScanValidatorConfig()


COMMON_SCAN_VALIDATORS = [
    ScanValidatorFactory.get(label) for label in COMMON_SCAN_VALIDATOR_LABELS
]


def validate_tiff(
    image_data,
    scan_validators: typing.List[typing.Any] = COMMON_SCAN_VALIDATORS,
    **kwargs,
) -> typing.List:
    """Ensure provided image data contains
    required metadata information concerning
    resolution and alike

    High-level validation API to trigger
    comma-separated labelled Validators
    """

    invalids = []
    if isinstance(image_data, str):
        image_data = Path(image_data)
    if isinstance(image_data, Path):
        the_image = InputImage(image_data)
        image_data = the_image
    if not isinstance(image_data, InputImage):
        raise InvalidImageDataException(f"Invalid {image_data}: must be InputImage or Path.")
    if image_data.metadata is None:
        image_data.read()
    for scan_validator in scan_validators:
        if isinstance(scan_validator, str):
            validator_class = ScanValidatorFactory.get_class(scan_validator)
            validator_instance = validator_class(image_data, **kwargs)
            if not validator_instance.is_valid():
                invalids.extend(validator_instance.invalids)
        if inspect.isclass(scan_validator):
            validator_instance = scan_validator(image_data, **kwargs)
            if not validator_instance.is_valid():
                invalids.extend(validator_instance.invalids)
    return invalids
