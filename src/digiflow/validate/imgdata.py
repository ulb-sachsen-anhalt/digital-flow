"""Image / TIFF-data specific Validation API"""

import dataclasses
import datetime
import hashlib
import io
import json
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
    X_RESOLUTION: 'resolution_x',
    Y_RESOLUTION: 'resolution_y',
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
RES_300 = 300               # like 300 DPI
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
class ValidatorConfig:
    """Centralized configuration for all image validators

    This dataclass bundles all validation parameters together,
    providing type safety, documentation, and easy serialization.
    """

    # File validator
    valid_min_size: int = MIN_SCAN_FILESIZE

    # Channel validator
    valid_channels: typing.List[int] = dataclasses.field(
        default_factory=lambda: GREYSCALE_OR_RGB.copy()
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
        default_factory=lambda: PHOTOMETRICS.copy()
    )
    required_rgb_profile: str = ADOBE_PROFILE_NAME

    def to_dict(self) -> dict:
        """Convert configuration to dictionary for kwargs

        Returns:
            Dictionary representation of configuration
        """
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, config_dict: dict) -> 'ValidatorConfig':
        """Create ValidatorConfig from dictionary

        Args:
            config_dict: Dictionary with configuration values

        Returns:
            New ValidatorConfig instance
        """
        return cls(**config_dict)

    @classmethod
    def from_env(cls, prefix: str = "VALIDATOR_") -> 'ValidatorConfig':
        """Load configuration from environment variables

        Args:
            prefix: Prefix for environment variable names

        Returns:
            New ValidatorConfig instance with values from environment
        """
        config_dict = {}
        for key in cls.__dataclass_fields__:
            env_key = f"{prefix}{key.upper()}"
            if env_key in os.environ:
                # Parse value based on type
                value = os.environ[env_key]
                field_type = cls.__dataclass_fields__[key].type
                if 'List' in str(field_type):
                    config_dict[key] = json.loads(value)
                elif field_type == int:
                    config_dict[key] = int(value)
                else:
                    config_dict[key] = value
        return cls(**config_dict)


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
    resolution_x: typing.Optional[IFDRational] = None
    resolution_y: typing.Optional[IFDRational] = None
    resolution_unit = UNSET_NUMBR
    artist = dfc.UNSET_LABEL
    copyright = dfc.UNSET_LABEL
    software = dfc.UNSET_LABEL
    created = dfc.UNSET_LABEL  # EXIF TAG 306 DATE_TIME
    channel = UNSET_NUMBR  # EFIX TAG 258 BITSPERSAMPLE


@dataclasses.dataclass
class Image:
    """Store required data"""

    local_path: str
    file_size = 0
    time_stamp = None
    profile = dfc.UNSET_LABEL
    check_sum_512 = dfc.UNSET_LABEL
    metadata: typing.Optional[ImageMetadata] = None
    invalids = []

    def read(self):
        """Try to read image data"""
        try:
            dt_object = datetime.datetime.now()
            self.time_stamp = dt_object.strftime(DATETIME_SRC_FORMAT)
            self.file_size = os.path.getsize(self.local_path)
            hash_val = hashlib.sha512()
            with open(self.local_path, "rb") as freader:
                hash_val.update(freader.read())
            self.check_sum_512 = hash_val.hexdigest()
            # read information from TIF TAG V2 section
            open_image = PILImage.open(self.local_path)
            pil_img: typing.Optional[TiffImageFile] = None
            if isinstance(open_image, TiffImageFile):
                pil_img = open_image
            else:
                msg = f'Not a valid TIFF image: {self.local_path}'
                raise InvalidImageDataException(msg)
            meta_data: ImageMetadata = self._read(pil_img)
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
        assert img_md is not None
        mds_res = f"({img_md.resolution_x},{img_md.resolution_y},{img_md.resolution_unit})"
        img_mds = f"\t{mds_res}\t{img_md.created}\t{img_md.artist}\t{img_md.copyright}\t{img_md.model}\t{img_md.software}\t{self.profile}"
        invalids = f"INVALID{self.invalids}" if len(self.invalids) > 0 else 'VALID'
        return f"{self.local_path}\t{self.check_sum_512}\t{img_fsize}\t{img_md.width}x{img_md.height}\t{img_mds}\t{self.time_stamp}\t{invalids}"


class ScanValidatorFile(Validator):
    """Validate file on very basic level, i.e.
    whether it can be read, file size suspicous
    or no metadata tags for width/height present
    """

    def __init__(self, input_data: Path, **kwargs):
        super().__init__(LABEL_SCAN_VALIDATOR_FILEDATA, input_data)
        if isinstance(self.input_data, Image):
            self.input_data = self.input_data.local_path
        self.min_size = kwargs.get('valid_min_size', MIN_SCAN_FILESIZE)

    def valid(self) -> bool:
        """Fail faster: If file can't even be read by group,
        """

        if not group_can_read(self.input_data):
            raise FSReadException(f'No permission to read {self.input_data}!')
        f_size = os.path.getsize(self.input_data)
        if f_size < self.min_size:
            msg = f"{INVALID_LABEL_RANGE} filesize {f_size}b < {self.min_size}b"
            self.invalids.append(Invalid(self.label, self.input_data, msg))
        input_image: Image = Image(self.input_data)
        input_image.read()
        assert input_image.metadata is not None
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

    def __init__(self, input_data: Image, **kwargs):
        super().__init__(LABEL_SCAN_VALIDATOR_CHANNEL, input_data)
        self.valid_channels = kwargs.get('valid_channels', GREYSCALE_OR_RGB)

    def valid(self) -> bool:
        input_image: Image = self.input_data
        assert input_image.metadata is not None
        img_md: ImageMetadata = input_image.metadata
        assert img_md is not None
        md_spp = img_md.samples_per_pixel
        if md_spp == dfc.UNSET_LABEL:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_UNSET} {LABEL_SAMPLES_PIXEL}"))
        elif md_spp not in self.valid_channels:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_SAMPLES_PIXEL} {md_spp} not in {self.valid_channels}"))
        if not all(c <= MAX_CHANNEL_DEPTH for c in img_md.channel):
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_CHANNEL} {img_md.channel} > {MAX_CHANNEL_DEPTH}"))
        return super().valid()


class ScanValidatorCompression(Validator):
    """Validate EXIF metadata uncompressed set"""

    def __init__(self, input_data: Image, **kwargs):
        super().__init__(LABEL_SCAN_VALIDATOR_COMPRESSION, input_data)

    def valid(self) -> bool:
        """Check flag umcompressed data set"""

        input_image: Image = self.input_data
        assert input_image.metadata is not None
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

    def __init__(self, input_data: Image, **kwargs):
        super().__init__(LABEL_SCAN_VALIDATOR_RESOLUTION, input_data, **kwargs)
        self.valid_resolutions = kwargs.get('valid_resolutions', [RES_300])

    def valid(self) -> bool:
        input_image: Image = self.input_data
        assert input_image is not None
        img_loc = input_image.local_path
        assert input_image.metadata is not None
        img_md: ImageMetadata = input_image.metadata
        a_prefix = self.label
        if img_md.resolution_unit == UNSET_NUMBR:
            inv01 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_UNIT}")
            self.invalids.append(inv01)
        elif img_md.resolution_unit != RES_UNIT_DPI:
            inv02 = Invalid(
                a_prefix, img_loc, f"{INVALID_LABEL_RANGE} {LABEL_RES_UNIT} {img_md.resolution_unit} != {RES_UNIT_DPI}")
            self.invalids.append(inv02)
        if img_md.resolution_x == UNSET_NUMBR:
            inv03 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_X}")
            self.invalids.append(inv03)
        else:
            if img_md.resolution_x not in self.valid_resolutions:
                inv04 = Invalid(a_prefix, img_loc,
                                f"{INVALID_LABEL_RANGE} {LABEL_RES_X}: {img_md.resolution_x}")
                self.invalids.append(inv04)
            if img_md.resolution_x is not None and isinstance(img_md.resolution_x, IFDRational):
                x_res: IFDRational = img_md.resolution_x
                if x_res.real.denominator != 1:
                    inv07 = Invalid(a_prefix, img_loc,
                                    f"{INVALID_LABEL_TYPE} {LABEL_RES_X}: {img_md.resolution_x}")
                    self.invalids.append(inv07)
        if img_md.resolution_y == UNSET_NUMBR:
            inv05 = Invalid(a_prefix, img_loc, f"{INVALID_LABEL_UNSET} {LABEL_RES_Y}")
            self.invalids.append(inv05)
        else:
            if img_md.resolution_y not in self.valid_resolutions:
                inv06 = Invalid(a_prefix, img_loc,
                                f"{INVALID_LABEL_RANGE} {LABEL_RES_Y}: {img_md.resolution_y}")
                self.invalids.append(inv06)
            if img_md.resolution_y is not None and isinstance(img_md.resolution_y, IFDRational):
                y_res: IFDRational = img_md.resolution_y
                if y_res.real.denominator != 1:
                    inv08 = Invalid(a_prefix, img_loc,
                                    f"{INVALID_LABEL_TYPE} {LABEL_RES_Y}: {img_md.resolution_y}")
                    self.invalids.append(inv08)
        return super().valid()


class ScanValidatorPhotometric(Validator):
    """Check EXIF Photometric information
    * Photometric interpretation must be Grayscale or RGB 
    * if RGB, then profile must be ADOBE RGB 1998
    """

    def __init__(self, input_data: Image, **kwargs):
        super().__init__(LABEL_SCAN_VALIDATOR_PHOTOMETRICS, input_data)
        self.valid_photometrics = kwargs.get('valid_photometrics', PHOTOMETRICS)

    def valid(self) -> bool:
        input_image: Image = self.input_data
        assert input_image.metadata is not None
        img_md: ImageMetadata = input_image.metadata
        pmetrics = img_md.photometric_interpretation
        if pmetrics not in self.valid_photometrics:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_GRAY_RGB} {pmetrics} not in {self.valid_photometrics}"))
        if pmetrics == 2 and input_image.profile != ADOBE_PROFILE_NAME:
            self.invalids.append(Invalid(self.label, input_image.local_path,
                                 f"{INVALID_LABEL_RANGE} {LABEL_PROFILE} != {ADOBE_PROFILE_NAME}"))
        return super().valid()


class ValidatorFactory:
    """Factory for creating and managing validator instances

    Supports configuration management and runtime registration of validators.
    """

    _registry: typing.ClassVar[typing.Dict[str, typing.Type[Validator]]] = {
        LABEL_SCAN_VALIDATOR_FILEDATA: ScanValidatorFile,
        LABEL_SCAN_VALIDATOR_CHANNEL: ScanValidatorChannel,
        LABEL_SCAN_VALIDATOR_COMPRESSION: ScanValidatorCompression,
        LABEL_SCAN_VALIDATOR_RESOLUTION: ScanValidatorResolution,
        LABEL_SCAN_VALIDATOR_PHOTOMETRICS: ScanValidatorPhotometric,
    }

    def __init__(self, config: typing.Optional[ValidatorConfig] = None):
        """Initialize factory with configuration

        Args:
            config: Configuration to use. Creates default if None.
        """
        self.config = config or ValidatorConfig()

    @classmethod
    def register(cls, validator_label: str, validator_class: typing.Type[Validator]) -> None:
        """Register a validator class

        Args:
            validator_label: Unique identifier for the validator
            validator_class: Class that inherits from Validator

        Raises:
            TypeError: If validator_class doesn't inherit from Validator
        """
        if not issubclass(validator_class, Validator):
            raise TypeError(f"{validator_class} must inherit from Validator")
        cls._registry[validator_label] = validator_class

    @classmethod
    def unregister(cls, validator_label: str) -> None:
        """Remove a validator from the registry"""
        cls._registry.pop(validator_label, None)

    @classmethod
    def get_class(cls, validator_label: str) -> typing.Type[Validator]:
        """Get validator class for label

        Args:
            validator_label: Label of the validator to retrieve

        Returns:
            The validator class

        Raises:
            KeyError: If validator_label not found
        """
        if validator_label not in cls._registry:
            available = ', '.join(cls._registry.keys())
            raise KeyError(
                f"No validator registered for '{validator_label}'. "
                f"Available validators: {available}"
            )
        return cls._registry[validator_label]

    @classmethod
    def get(cls, validator_label: str) -> typing.Type[Validator]:
        """Get validator class for label (legacy method for backward compatibility)

        Args:
            validator_label: Label of the validator to retrieve

        Returns:
            The validator class

        Raises:
            NotImplementedError: If validator_label not found
        """
        if validator_label not in cls._registry:
            raise NotImplementedError(f"No implementation for {validator_label}!")
        return cls._registry[validator_label]

    def create(self, validator_label: str, input_data: typing.Any,
               override_config: typing.Optional[dict] = None, **kwargs) -> Validator:
        """Create a validator instance with configuration

        Args:
            validator_label: Validator to create
            input_data: Input for validator
            override_config: Config overrides for this instance only
            **kwargs: Additional kwargs (for backward compatibility)

        Returns:
            Configured validator instance
        """
        validator_class = self.get_class(validator_label)

        # Merge default config with overrides and kwargs
        config_dict = self.config.to_dict()
        if override_config:
            config_dict.update(override_config)
        if kwargs:
            config_dict.update(kwargs)

        return validator_class(input_data, **config_dict)

    def update_config(self, **kwargs) -> None:
        """Update factory configuration

        Args:
            **kwargs: Configuration parameters to update
        """
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

    @classmethod
    def list_validators(cls) -> typing.List[str]:
        """Get list of all registered validator labels"""
        return list(cls._registry.keys())

    @classmethod
    def has_validator(cls, validator_label: str) -> bool:
        """Check if a validator is registered"""
        return validator_label in cls._registry

    @property
    def validators(self) -> typing.Dict[str, typing.Type[Validator]]:
        """Property for backward compatibility with old code"""
        return self._registry


class ScanValidators(Validator):
    """Encapsulate image validation"""

    def __init__(self, path_input: Path, validator_labels: typing.List[str],
                 validator_factory: typing.Optional[ValidatorFactory] = None, **kwargs):
        super().__init__(LABEL_SCAN_VALIDATOR_COMBINED, path_input)
        self.path_input = path_input
        self.validator_labels = validator_labels
        self.validator_factory = validator_factory or ValidatorFactory()
        self.img_data: typing.Optional[Image] = None
        self.kwargs = kwargs

    def valid(self) -> bool:
        self.img_data = Image(self.input_data)
        self.img_data.read()
        for label in self.validator_labels:
            validator = self.validator_factory.create(label, self.img_data, **self.kwargs)
            if not validator.valid():
                self.invalids.extend(validator.invalids)
        return super().valid()


def validate_tiff(tif_path, required_validators: typing.Optional[typing.List[str]] = None, **kwargs) -> ScanValidators:
    """Ensure provided image data contains
    required metadata information concerning
    resolution and alike

    High-level validation API to trigger
    comma-separated labelled Validators
    """

    if required_validators is None:
        required_validators = list(ValidatorFactory._registry.keys())
    assert required_validators is not None
    image_val = ScanValidators(tif_path, required_validators, **kwargs)
    image_val.valid()
    return image_val
