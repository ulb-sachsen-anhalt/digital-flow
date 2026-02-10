"""API for validation on file-system-level"""

import shutil
import typing

from pathlib import Path

import pytest

import digiflow.validate as df_v
import digiflow.validate.imgdata as df_vi

from .conftest import TEST_RES


def test_tiff_channel_depth_invalid(tmp_path):
    """What happens if feed a tiff
    with (at least) invalid channel size?
    """

    # arrange
    file_name = '43837_max_01.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(tmp_path, file_name)
    shutil.copy(file_source, file_target)

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(file_target)

    # assert
    assert len(invalids) == 1
    inv01 = invalids[0]
    assert inv01.label == df_v.LABEL_SCAN_VALIDATOR_CHANNEL
    assert inv01.location == file_target
    assert f'{df_v.INVALID_LABEL_RANGE} {df_vi.LABEL_CHANNEL} (16, 16, 16) > 8' == inv01.info


def test_tiff_resolution_invalid(tmp_path):
    """What happens if feed a tiff
    with floating point resolution information?
    """

    # arrange
    file_name = '8736_max_01.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(file_target)

    # assert
    assert len(invalids) == 4
    assert str(invalids[0].location).endswith(file_name)
    assert f'{df_v.INVALID_LABEL_RANGE} {df_vi.LABEL_RES_X}: 470.55' == invalids[0].info
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_X}: 470.55' == invalids[1].info
    assert f'{df_v.INVALID_LABEL_RANGE} {df_vi.LABEL_RES_Y}: 470.55' == invalids[2].info
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_Y}: 470.55' == invalids[3].info


def test_tiff_resolution_invalid_alter_range(tmp_path):
    """Behavior when altering valid resolution range
    to ignore resolution 470.55 dpi of the image
    """

    # arrange
    file_name = '8736_max_01.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(
        file_target, valid_resolutions=[300, 470.55]
    )

    # assert
    assert len(invalids) == 2
    assert str(invalids[0].location).endswith(file_name)
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_X}: 470.55' == invalids[0].info
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_Y}: 470.55' == invalids[1].info


def test_tiff_validate_only_channels_valid(tmp_path):
    """Ensure that validation can be controlled
    by provided labels so in this case only
    channels are validate (although image still
    contains invalid resolution tags)
    """

    # arrange
    file_name = '8736_max_01.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(
        file_target, [df_v.LABEL_SCAN_VALIDATOR_CHANNEL]
    )

    # assert
    assert len(invalids) == 0


@pytest.fixture(name='img_resolution_invalid')
def _fixture_img_resolution_invalid(tmp_path):
    file_name = '8736_max_01.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = tmp_path / file_name
    shutil.copy(file_source, file_target)
    img = df_v.InputImage(file_target)
    img.read()
    yield img


def test_tiffexifresolution_resolution_invalid(img_resolution_invalid):
    """Ensure only invalid resolution numerical type fraction recognized"""

    # arrange
    tiff_exif_val = df_v.ScanValidatorResolution(img_resolution_invalid,
                                                 valid_resolutions=[300, 470.55])
    # act
    tiff_exif_val.check()

    # assert
    assert tiff_exif_val.label == df_v.LABEL_SCAN_VALIDATOR_RESOLUTION
    assert len(tiff_exif_val.invalids) == 2
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_X}: 470.55' == tiff_exif_val.invalids[0].info
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_Y}: 470.55' == tiff_exif_val.invalids[1].info


def test_tiffexifresolution_channels_valid(img_resolution_invalid):
    """Ensure channel data is valid instead of resolution data"""

    # arrange
    tiff_exif_val = df_v.ScanValidatorChannel(img_resolution_invalid)

    # act
    tiff_exif_val.check()

    # assert
    assert tiff_exif_val.label == df_v.LABEL_SCAN_VALIDATOR_CHANNEL
    assert len(tiff_exif_val.invalids) == 0


def test_tiff_grayscale_newspaper_valid(tmp_path):
    """Ensure grayscale image properly recognized
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(file_target,
                                                             valid_resolutions=[470])
    img_data = df_v.InputImage(file_target)
    img_data.read()

    # assert
    assert len(invalids) == 0
    assert img_data.metadata is not None
    assert img_data.metadata.channel == (8,)
    assert img_data.metadata.resolution_x == 470
    assert img_data.metadata.resolution_y == 470
    assert img_data.metadata.resolution_unit == 2
    assert str(img_data.input_path).endswith(file_name)


def test_tiff_grayscale_newspaper_only_scanfiledata_valid(tmp_path):
    """Prevent regression: don't map validator to
    'TiffImageFile' since this a class from 
    'PIL.TiffImagePlugin' and all will crack
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = tmp_path / file_name
    shutil.copy(file_source, file_target)

    # act
    sf_validator_clazz: typing.Type[df_v.Validator] = df_vi.ScanValidatorFactory.get(
        df_v.LABEL_SCAN_VALIDATOR_FILE)
    assert sf_validator_clazz is not None
    validator: df_v.Validator = sf_validator_clazz(file_target)

    # assert
    validator.check()
    assert len(validator.invalids) == 0
    assert validator.label == df_v.LABEL_SCAN_VALIDATOR_FILE
    assert validator.input_file is not None
    assert validator.input_file.input_path == file_target


def test_tiff_grayscale_newspaper_default_validators(tmp_path):
    """2025-11-10: Changed behavior when altering valid resolution range
    to exclude the image resolution of 470 dpi
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = tmp_path / file_name
    shutil.copy(file_source, file_target)
    validator_labels = [df_v.LABEL_SCAN_VALIDATOR_FILE, df_v.LABEL_SCAN_VALIDATOR_RESOLUTION]

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(
        file_target, validator_labels
    )

    # assert
    assert len(invalids) > 0


def test_validate_tiff_with_input_image(tmp_path):
    """Ensure validate_tiff can accept InputImage instance directly and properly read it
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = tmp_path / file_name
    shutil.copy(file_source, file_target)
    validator_labels = [df_v.LABEL_SCAN_VALIDATOR_FILE, df_v.LABEL_SCAN_VALIDATOR_RESOLUTION]

    # act
    input_file = df_vi.InputImage(file_target)  # Pre-read to ensure metadata is populated
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(
        input_file, validator_labels
    )

    # assert
    assert len(invalids) > 0


def test_tiff_grayscale_newspaper_custom_validators_valid(tmp_path):
    """2025-11-10: Changed behavior when altering valid resolution range
    to exclude the image resolution of 470 dpi => requires to add 470 dpi
    to valid resolutions to make the image pass validation
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)
    validator_labels = [df_v.LABEL_SCAN_VALIDATOR_FILE, df_v.LABEL_SCAN_VALIDATOR_RESOLUTION]

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(
        file_target, validator_labels, valid_resolutions=[470]
    )

    # assert
    assert len(invalids) == 0


def test_tiff_resolution_missing(tmp_path):
    """Detect invalid images:
    What happens if resolution values completely missing?
    """

    # arrange
    file_name = '8736_max_02.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    invalids: typing.List[df_v.Invalid] = df_v.validate_tiff(file_target)

    # assert
    assert len(invalids) == 3
    assert str(invalids[0].location).endswith(file_name)
    assert f'{df_v.INVALID_LABEL_UNSET} {df_vi.LABEL_RES_UNIT}' == invalids[0].info
    assert f'{df_v.INVALID_LABEL_RANGE} {df_vi.LABEL_RES_X}: None' == invalids[1].info
    assert f'{df_v.INVALID_LABEL_RANGE} {df_vi.LABEL_RES_Y}: None' == invalids[2].info


@pytest.mark.parametrize(['image_name', 'check_sum', 'x_resolution'],
                         [('8736_max_01.tif', 'af097aeb37e311895bb189710cfd98e8671dba05b2f02ce4cd196d74d856a699f7dcd1afd0369049f9f30e6442412acd78a6d67fb8c71cf2147520c1264a6b0f', 470.55),
                          ('8736_max_02.tif', '9d3fad68d0c4b1a8e2a1c9b8b14c6546fb980f81b9d427ddb07a173ec83d4e979a5881a1a1803776f6477d1729dd971204cce465e3f2ccb1612ffa8bdcb2fed0', None),
                          ('1667522809_J_0025_0512.tif', '6b8ad086a5719da8a7f9a951ddca9f93843ca99fdc2b8f06bd28db89367af115e616cb76522d946fdd714e35acfbcbc7ae67c06635903316fe330d7e7bd6103a', 470),
                          ("43837_max_01.tif", "4b1f31b0f757f83f73244895c63c2c9c9f1ec488f3d46b214d97974628aa51b2208c88313c4dbc79d942d36203465733fa77ca1f931eb570b14d2f4eae721755", 300)])
def test_tiff_image_properties(image_name, check_sum, x_resolution):
    """Make sure image properties read properly"""

    # arrange
    file_source = Path(TEST_RES) / 'image' / image_name

    # act
    image: df_v.InputImage = df_v.InputImage(file_source)
    image.read()

    # assert
    assert image.metadata
    assert image.check_sum_512 == check_sum
    assert image.metadata.resolution_x == x_resolution


# ============================================================================
# ValidatorConfig Tests - pytest style
# ============================================================================

@pytest.fixture(name='default_config')
def _fixture_default_config():
    """Fixture providing a default ValidatorConfig instance"""
    return df_vi.ScanValidatorConfig()


@pytest.fixture(name='custom_config')
def _fixture_custom_config():
    """Fixture providing a custom ValidatorConfig instance"""
    return df_vi.ScanValidatorConfig(
        valid_min_size=2048,
        valid_channels=[1, 3, 4],
        max_channel_depth=16,
        valid_resolutions=[300, 400, 600],
        required_rgb_profile="sRGB IEC61966-2.1"
    )


@pytest.fixture(name='newspaper_image')
def _fixture_newspaper_image(tmp_path):
    """Fixture providing path to newspaper test image"""
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)
    return file_target


def test_validator_config_default_initialization(default_config: df_vi.ScanValidatorConfig):
    """Test ValidatorConfig creates with default values"""
    assert default_config.valid_min_size == df_vi.MIN_SCAN_FILESIZE
    assert default_config.valid_channels == df_vi.GREYSCALE_OR_RGB
    assert default_config.max_channel_depth == df_vi.MAX_CHANNEL_DEPTH
    assert default_config.valid_compression == df_vi.DEFAULT_COMPRESSION
    assert default_config.valid_resolutions == [df_vi.RES_300]
    assert default_config.valid_resolution_unit == df_vi.RES_UNIT_DPI
    assert default_config.valid_photometrics == df_vi.PHOTOMETRICS
    assert default_config.required_rgb_profile == df_vi.ADOBE_PROFILE_NAME


def test_validator_config_custom_initialization(custom_config: df_vi.ScanValidatorConfig):
    """Test ValidatorConfig with custom values"""
    assert custom_config.valid_min_size == 2048
    assert custom_config.valid_channels == [1, 3, 4]
    assert custom_config.max_channel_depth == 16
    assert custom_config.valid_resolutions == [300, 400, 600]
    assert custom_config.required_rgb_profile == "sRGB IEC61966-2.1"
    # Check defaults remain unchanged
    assert custom_config.valid_compression == df_vi.DEFAULT_COMPRESSION
    assert custom_config.valid_resolution_unit == df_vi.RES_UNIT_DPI


def test_validator_config_to_dict(custom_config: df_vi.ScanValidatorConfig):
    """Test conversion of ValidatorConfig to dictionary"""
    config_dict = custom_config.to_dict()

    assert isinstance(config_dict, dict)
    assert config_dict['valid_min_size'] == 2048
    assert config_dict['valid_resolutions'] == [300, 400, 600]
    assert 'valid_channels' in config_dict
    assert 'max_channel_depth' in config_dict


def test_validator_config_from_dict():
    """Test creation of ValidatorConfig from dictionary"""
    config_dict = {
        'valid_min_size': 4096,
        'valid_channels': [3],
        'max_channel_depth': 12,
        'valid_resolutions': [600, 1200]
    }

    config = df_vi.ScanValidatorConfig.from_dict(config_dict)

    assert config.valid_min_size == 4096
    assert config.valid_channels == [3]
    assert config.max_channel_depth == 12
    assert config.valid_resolutions == [600, 1200]


def test_validator_config_roundtrip_dict():
    """Test that to_dict and from_dict preserve config"""
    original = df_vi.ScanValidatorConfig(
        valid_min_size=8192,
        valid_resolutions=[300, 600, 1200],
        required_rgb_profile="ProPhoto RGB"
    )

    config_dict = original.to_dict()
    restored = df_vi.ScanValidatorConfig.from_dict(config_dict)

    assert restored.valid_min_size == original.valid_min_size
    assert restored.valid_resolutions == original.valid_resolutions
    assert restored.required_rgb_profile == original.required_rgb_profile


def test_validator_config_immutability():
    """Test that default list values are not shared between instances"""
    config1 = df_vi.ScanValidatorConfig()
    config2 = df_vi.ScanValidatorConfig()

    # Modify config1's list
    config1.valid_resolutions.append(600)

    assert 600 in config1.valid_resolutions
    assert 600 not in config2.valid_resolutions


def test_validator_config_with_validator_factory(newspaper_image):
    """Test using ValidatorConfig with ValidatorFactory"""
    config = df_vi.ScanValidatorConfig(valid_resolutions=[470])
    factory = df_vi.ScanValidatorFactory(config)

    img = df_v.InputImage(newspaper_image)
    img.read()
    validator = factory.create(df_v.LABEL_SCAN_VALIDATOR_RESOLUTION, img)

    validator.check()
    assert len(validator.invalids) == 0


def test_validator_config_override_in_factory(newspaper_image):
    """Test overriding config in factory.create()"""
    config = df_vi.ScanValidatorConfig(valid_resolutions=[300])  # Won't match 470
    factory = df_vi.ScanValidatorFactory(config)

    img = df_v.InputImage(newspaper_image)
    img.read()
    validator = factory.create(
        df_v.LABEL_SCAN_VALIDATOR_RESOLUTION,
        img,
        override_config={'valid_resolutions': [470]}  # Override to match
    )

    validator.check()
    assert len(validator.invalids) == 0


def test_validator_factory_initialization():
    """Test ValidatorFactory initialization with and without config"""
    # Without config
    factory1 = df_vi.ScanValidatorFactory()
    assert factory1.config is not None
    assert isinstance(factory1.config, df_vi.ScanValidatorConfig)

    # With config
    custom_config = df_vi.ScanValidatorConfig(valid_resolutions=[600])
    factory2 = df_vi.ScanValidatorFactory(custom_config)
    assert factory2.config.valid_resolutions == [600]


def test_validator_factory_update_config():
    """Test updating ValidatorFactory configuration"""
    factory = df_vi.ScanValidatorFactory(df_vi.ScanValidatorConfig())
    original_resolutions = factory.config.valid_resolutions.copy()

    factory.update_config(valid_resolutions=[600, 1200], max_channel_depth=16)

    assert factory.config.valid_resolutions != original_resolutions
    assert factory.config.valid_resolutions == [600, 1200]
    assert factory.config.max_channel_depth == 16


@pytest.mark.parametrize("validator_label", [
    df_v.LABEL_SCAN_VALIDATOR_FILE,
    df_v.LABEL_SCAN_VALIDATOR_CHANNEL,
    df_v.LABEL_SCAN_VALIDATOR_COMPRESSION,
    df_v.LABEL_SCAN_VALIDATOR_RESOLUTION,
    df_v.LABEL_SCAN_VALIDATOR_PHOTOMETRICS,
])
def test_validator_factory_has_all_validators(validator_label):
    """Test that ValidatorFactory has all required validators registered"""
    assert df_vi.ScanValidatorFactory.has_validator(validator_label)


def test_validator_factory_list_validators():
    """Test listing all registered validators"""
    validators = df_vi.ScanValidatorFactory.list_validators()

    assert isinstance(validators, list)
    assert len(validators) >= 5
    assert df_v.LABEL_SCAN_VALIDATOR_RESOLUTION in validators
