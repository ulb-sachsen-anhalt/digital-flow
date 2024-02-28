"""Validate data on file-level"""

import stat

from pathlib import (
    Path,
)


PERMISSION_GROUP_READ = [stat.S_IRGRP]
PERMISSION_GROUP_READ_WRITE = [stat.S_IRGRP, stat.S_IWGRP]


class FSReadException(Exception):
    """Mark State process can't even 
    read file resource due ownership
    """


class FSWriteException(Exception):
    """Mark state process tries to write
    resource for modifications and/or
    sanitizing but isn't allowed
    to store data due ownership 
    restrictions"""


def _check_modus(res_path, fs_modi=None):
    """filesystem provides detailed
    information about access permissions
    check on bit-level where *all* requested
    file modi *must* match
    """

    if isinstance(res_path, str):
        res_path = Path(res_path)
    stat_result = res_path.stat()
    _fs_modi = stat_result.st_mode
    if fs_modi is None:
        fs_modi = PERMISSION_GROUP_READ
    return all(_fs_modi & m for m in fs_modi)


def resource_can_be(res_path, modi):
    """Check whether resource can
    be read/write/executed as required
    """

    return _check_modus(res_path, modi)


def group_can_read(res_path) -> bool:
    """Inspect whether resource can be read
    at least"""

    return resource_can_be(res_path, PERMISSION_GROUP_READ)


def group_can_write(res_path) -> bool:
    """Inspect whether resouce could be written
    in case to persist modifications"""

    return resource_can_be(res_path, PERMISSION_GROUP_READ_WRITE)
