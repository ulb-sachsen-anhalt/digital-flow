"""API for validation on file-system-level"""

import shutil

from pathlib import (
    Path,
)

import pytest

from digiflow.validate import (
    INVALID_LABEL_RANGE,
    INVALID_LABEL_TYPE,
    LABEL_VALIDATOR_SCAN_CHANNEL,
    LABEL_VALIDATOR_SCAN_FILEDATA,
    LABEL_VALIDATOR_SCAN_RESOLUTION,
    Image,
    CombinedScanValidator,
    ScanChannelValidator,
    ScanResolutionValidator,
    Validator,
    ValidatorFactory,
    validate_tiff,
)

from .conftest import (
    TEST_RES,
)


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
    _outcomes = validate_tiff(file_target)

    # assert
    assert len(_outcomes.invalids) == 1
    _inv = _outcomes.invalids[0]
    assert _inv.label == LABEL_VALIDATOR_SCAN_CHANNEL
    assert _inv.location == file_target
    assert f'{INVALID_LABEL_RANGE} channel (16, 16, 16) > 8' == _inv.info


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
    _outcomes:CombinedScanValidator = validate_tiff(file_target)

    # assert
    assert len(_outcomes.invalids) == 2
    assert str(_outcomes.path_input).endswith(file_name)
    assert f'{INVALID_LABEL_TYPE} xRes: 470.55' == _outcomes.invalids[0].info
    assert f'{INVALID_LABEL_TYPE} yRes: 470.55' == _outcomes.invalids[1].info


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
    _outcomes:CombinedScanValidator = validate_tiff(file_target, [LABEL_VALIDATOR_SCAN_CHANNEL])

    # assert
    assert len(_outcomes.invalids) == 0


@pytest.fixture(name='img_resolution_invalid')
def _fixture_img_resolution_invalid(tmp_path):
    file_name = '8736_max_01.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)
    _img = Image(file_target)
    _img.read()
    yield _img


def test_tiffexifresolution_resolution_invalid(img_resolution_invalid):
    """Ensure invalid resolution is recognized"""

    # arrange
    _tiff_exif_val = ScanResolutionValidator(img_resolution_invalid)

    # act
    _tiff_exif_val.valid()

    # assert
    assert _tiff_exif_val.label == LABEL_VALIDATOR_SCAN_RESOLUTION
    assert len(_tiff_exif_val.invalids) == 2
    assert f'{INVALID_LABEL_TYPE} xRes: 470.55' == _tiff_exif_val.invalids[0].info
    assert f'{INVALID_LABEL_TYPE} yRes: 470.55' == _tiff_exif_val.invalids[1].info
    

def test_tiffexifresolution_channels_valid(img_resolution_invalid):
    """Ensure channel data is valid this time"""

    # arrange
    _tiff_exif_val = ScanChannelValidator(img_resolution_invalid)

    # act
    _tiff_exif_val.valid()

    # assert
    assert _tiff_exif_val.label == LABEL_VALIDATOR_SCAN_CHANNEL
    assert len(_tiff_exif_val.invalids) == 0


def test_tiff_grayscale_newspaper_defaults_valid(tmp_path):
    """Ensure grayscale image properly recognized
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    _img_data: Image = validate_tiff(file_target).img_data

    # assert
    assert _img_data.metadata.channel == (8,)
    assert _img_data.metadata.xRes == 470
    assert _img_data.metadata.yRes == 470
    assert _img_data.metadata.resolution_unit == 2
    assert str(_img_data.url).endswith(file_name)


def test_tiff_grayscale_newspaper_valid(tmp_path):
    """Ensure grayscale image properly recognized
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    _img_data: Image = validate_tiff(file_target).img_data

    # assert
    assert _img_data.metadata.channel == (8,)
    assert _img_data.metadata.xRes == 470
    assert _img_data.metadata.yRes == 470
    assert _img_data.metadata.resolution_unit == 2
    assert str(_img_data.url).endswith(file_name)


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
    _scan_file_validator_clazz = ValidatorFactory.get(LABEL_VALIDATOR_SCAN_FILEDATA)
    _validator: Validator = _scan_file_validator_clazz(file_target)

    # assert
    assert _validator.valid()
    assert _validator.label == LABEL_VALIDATOR_SCAN_FILEDATA
    assert _validator.input_data == file_target


def test_tiff_grayscale_newspaper_custom_validators_valid(tmp_path):
    """Don't mix input data, otherwise something pops up like:
    AttributeError: 'Image' object has no attribute 'stat'
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)
    _validator_labels = [LABEL_VALIDATOR_SCAN_FILEDATA, LABEL_VALIDATOR_SCAN_RESOLUTION]

    # act
    _val = validate_tiff(file_target, _validator_labels)

    # assert
    assert _val.valid()
