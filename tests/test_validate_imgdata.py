"""API for validation on file-system-level"""

import shutil

from pathlib import (
    Path,
)

import pytest

from digiflow.validate import (
    LABEL_VALIDATOR_SCAN_CHANNEL,
    LABEL_VALIDATOR_SCAN_RESOLUTION,
    Image,
    CombinedScanValidator,
    ScanChannelValidator,
    ScanResolutionValidator,
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
    assert 'channel: (16, 16, 16)' == _outcomes.invalids[0].info


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
    assert 'xRes: 470.55' == _outcomes.invalids[0].info
    assert 'yRes: 470.55' == _outcomes.invalids[1].info


def test_tiff_validate_by_labels(tmp_path):
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


def test_tiffexifresolution_resolution_not_integral(img_resolution_invalid):
    """Ensure invalid resolution is recognized"""

    # arrange
    _tiff_exif_val = ScanResolutionValidator(img_resolution_invalid)

    # act
    _tiff_exif_val.valid()

    # assert
    assert _tiff_exif_val.label == LABEL_VALIDATOR_SCAN_RESOLUTION
    assert len(_tiff_exif_val.invalids) == 2
    assert 'xRes: 470.55' == _tiff_exif_val.invalids[0].info
    assert 'yRes: 470.55' == _tiff_exif_val.invalids[1].info
    

def test_tiffexifresolution_channels_ok(img_resolution_invalid):
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
