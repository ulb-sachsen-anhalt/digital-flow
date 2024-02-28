"""API for validation on file-system-level"""

import os
import shutil
import stat

from pathlib import (
    Path,
)

from digiflow.validate import (
    group_can_read,
    group_can_write,
    resource_can_be,
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
