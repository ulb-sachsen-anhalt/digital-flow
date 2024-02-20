"""API for validation on file-system-level"""

import os
import shutil
import stat

from pathlib import (
    Path,
)

import pytest

from digiflow.validate import (
    Image,
    ImageInvalidException,
    group_can_read,
    group_can_write,
    resource_can_be,
    validate_tiff,
)

from .conftest import (
    TEST_RES,
)


def test_can_read_common_file(tmp_path):
    """Ensure temporay test data has expected
    file attributes and can therefore be read
    """

    # arrange
    file_name = 'k2_mets_morbio_1748529021.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(str(mets_source), str(mets_target))

    # act
    assert mets_target.exists()
    assert group_can_read(mets_target)
    # user and group may read
    assert resource_can_be(mets_target, [stat.S_IRUSR, stat.S_IRGRP])
    # user and group and even others may read
    assert resource_can_be(mets_target, [stat.S_IRUSR, stat.S_IRGRP, stat.S_IROTH])
    # but others are not allowed to write
    assert not resource_can_be(mets_target, [stat.S_IWOTH])
    assert not resource_can_be(
        mets_target, [stat.S_IRUSR, stat.S_IRGRP, stat.S_IROTH, stat.S_IWOTH])


def test_group_cant_access_resource(tmp_path):
    """Try to construct common scenario where
    resource cant be accessed by the group
    but only be the user
    """

    # arrange
    file_name = 'k2_mets_morbio_1748529021.xml'
    mets_source = Path(TEST_RES) / file_name
    mets_target = Path(str(tmp_path), file_name)
    shutil.copy(mets_source, mets_target)

    # act
    os.chmod(mets_target, stat.S_IRUSR | stat.S_IWUSR)

    # assert
    assert not group_can_read(mets_target)
    assert not group_can_write(mets_target)
    assert resource_can_be(mets_target, [stat.S_IRUSR, stat.S_IWUSR])


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
    with pytest.raises(ImageInvalidException) as iiexc:
        validate_tiff(file_target)

    # assert
    assert file_name in iiexc.value.args[0]
    assert "'channel: (16, 16, 16)'" in iiexc.value.args[0]


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
    with pytest.raises(ImageInvalidException) as iiexc:
        validate_tiff(file_target)

    # assert
    assert file_name in iiexc.value.args[0]
    assert "'xRes: 470.55', 'yRes: 470.55'" in iiexc.value.args[0]


def test_tiff_grayscale_newspaper_valid(tmp_path):
    """Ensure grayscale image properly recognized
    """

    # arrange
    file_name = '1667522809_J_0025_0512.tif'
    file_source = Path(TEST_RES) / 'image' / file_name
    file_target = Path(str(tmp_path), file_name)
    shutil.copy(file_source, file_target)

    # act
    _img_data: Image = validate_tiff(file_target)

    # assert
    assert _img_data.metadata.channel == (8,)
    assert _img_data.metadata.xRes == 470
    assert _img_data.metadata.yRes == 470
    assert _img_data.metadata.resolution_unit == 2
    assert str(_img_data.url).endswith(file_name)

