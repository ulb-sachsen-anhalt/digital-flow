"""API for validation on file-system-level"""

import shutil

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
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    outcomes = df_v.validate_tiff(file_target)

    # assert
    assert len(outcomes.invalids) == 1
    inv01 = outcomes.invalids[0]
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
    outcomes: df_v.ScanValidatorCombined = df_v.validate_tiff(file_target)

    # assert
    assert len(outcomes.invalids) == 2
    assert str(outcomes.path_input).endswith(file_name)
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_X}: 470.55' == outcomes.invalids[0].info
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_Y}: 470.55' == outcomes.invalids[1].info


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
    outcomes: df_v.ScanValidatorCombined = df_v.validate_tiff(file_target, [df_v.LABEL_SCAN_VALIDATOR_CHANNEL])

    # assert
    assert len(outcomes.invalids) == 0


@pytest.fixture(name='img_resolution_invalid')
def _fixture_img_resolution_invalid(tmp_path):
    file_name = '8736_max_01.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)
    img = df_v.Image(file_target)
    img.read()
    yield img


def test_tiffexifresolution_resolution_invalid(img_resolution_invalid):
    """Ensure invalid resolution is recognized"""

    # arrange
    tiff_exif_val = df_v.ScanValidatorResolution(img_resolution_invalid)

    # act
    tiff_exif_val.valid()

    # assert
    assert tiff_exif_val.label == df_v.LABEL_SCAN_VALIDATOR_RESOLUTION
    assert len(tiff_exif_val.invalids) == 2
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_X}: 470.55' == tiff_exif_val.invalids[0].info
    assert f'{df_v.INVALID_LABEL_TYPE} {df_vi.LABEL_RES_Y}: 470.55' == tiff_exif_val.invalids[1].info


def test_tiffexifresolution_channels_valid(img_resolution_invalid):
    """Ensure channel data is valid this time"""

    # arrange
    tiff_exif_val = df_v.ScanValidatorChannel(img_resolution_invalid)

    # act
    tiff_exif_val.valid()

    # assert
    assert tiff_exif_val.label == df_v.LABEL_SCAN_VALIDATOR_CHANNEL
    assert len(tiff_exif_val.invalids) == 0


def test_tiff_grayscale_newspaper_defaults_valid(tmp_path):
    """Ensure grayscale image properly recognized
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    img_data: df_v.Image = df_v.validate_tiff(file_target).img_data

    # assert
    assert img_data.metadata.channel == (8,)
    assert img_data.metadata.xRes == 470
    assert img_data.metadata.yRes == 470
    assert img_data.metadata.resolution_unit == 2
    assert str(img_data.url).endswith(file_name)


def test_tiff_grayscale_newspaper_valid(tmp_path):
    """Ensure grayscale image properly recognized
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    img_data: df_v.Image = df_v.validate_tiff(file_target).img_data

    # assert
    assert img_data.metadata.channel == (8,)
    assert img_data.metadata.xRes == 470
    assert img_data.metadata.yRes == 470
    assert img_data.metadata.resolution_unit == 2
    assert str(img_data.url).endswith(file_name)


def test_tiff_grayscale_newspaper_only_scanfiledata_valid(tmp_path):
    """Prevent regression: don't map validator to
    'TiffImageFile' since this a class from 
    'PIL.TiffImagePlugin' and all will crack
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    scan_file_validator_clazz = df_v.ValidatorFactory.get(df_v.LABEL_SCAN_VALIDATOR_FILEDATA)
    validator: df_v.Validator = scan_file_validator_clazz(file_target)

    # assert
    assert validator.valid()
    assert validator.label == df_v.LABEL_SCAN_VALIDATOR_FILEDATA
    assert validator.input_data == file_target


def test_tiff_grayscale_newspaper_custom_validators_valid(tmp_path):
    """Don't mix input data, otherwise something pops up like:
    AttributeError: 'Image' object has no attribute 'stat'
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)
    validator_labels = [df_v.LABEL_SCAN_VALIDATOR_FILEDATA, df_v.LABEL_SCAN_VALIDATOR_RESOLUTION]

    # act
    result = df_v.validate_tiff(file_target, validator_labels)

    # assert
    assert result.valid()


def test_tiff_resolution_missing(tmp_path):
    """What happens if resolution values are 
    completely missing?
    """

    # arrange
    file_name = '8736_max_02.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    outcomes: df_v.ScanValidatorCombined = df_v.validate_tiff(file_target)

    # assert
    assert len(outcomes.invalids) == 3
    assert str(outcomes.path_input).endswith(file_name)
    assert f'{df_v.INVALID_LABEL_UNSET} {df_vi.LABEL_RES_UNIT}' == outcomes.invalids[0].info
    assert f'{df_v.INVALID_LABEL_UNSET} {df_vi.LABEL_RES_X}' == outcomes.invalids[1].info
    assert f'{df_v.INVALID_LABEL_UNSET} {df_vi.LABEL_RES_Y}' == outcomes.invalids[2].info


@pytest.mark.parametrize(['image_name', 'check_sum', 'x_resolution'],
                         [('8736_max_01.tif', 'af097aeb37e311895bb189710cfd98e8671dba05b2f02ce4cd196d74d856a699f7dcd1afd0369049f9f30e6442412acd78a6d67fb8c71cf2147520c1264a6b0f', 470.55),
                          ('8736_max_02.tif', '9d3fad68d0c4b1a8e2a1c9b8b14c6546fb980f81b9d427ddb07a173ec83d4e979a5881a1a1803776f6477d1729dd971204cce465e3f2ccb1612ffa8bdcb2fed0', df_v.UNSET_NUMBR),
                          ('1667522809_J_0025_0512.tif', '6b8ad086a5719da8a7f9a951ddca9f93843ca99fdc2b8f06bd28db89367af115e616cb76522d946fdd714e35acfbcbc7ae67c06635903316fe330d7e7bd6103a', 470),
                          ("43837_max_01.tif", "4b1f31b0f757f83f73244895c63c2c9c9f1ec488f3d46b214d97974628aa51b2208c88313c4dbc79d942d36203465733fa77ca1f931eb570b14d2f4eae721755", 300)])
def test_tiff_image_properties(image_name, check_sum, x_resolution):
    """Make sure image properties read properly"""

    # arrange
    file_source = Path(TEST_RES) / 'image' / image_name

    # act
    image: df_v.Image = df_v.Image(file_source)
    image.read()

    # assert
    assert image.metadata
    assert image.check_sum_512 == check_sum
    assert image.metadata.xRes == x_resolution
