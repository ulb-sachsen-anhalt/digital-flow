import os

from pathlib import (
    Path
)


__FILE_ABS_PATH__ = os.path.abspath(__file__)
TEST_ROOT = Path(os.path.dirname(__FILE_ABS_PATH__))
TEST_RES = TEST_ROOT / 'resources'
LIB_RES = Path(__FILE_ABS_PATH__).parent.parent / 'src' / 'digiflow' / 'resources'
