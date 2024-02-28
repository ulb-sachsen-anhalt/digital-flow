"""Common Validation API"""

from abc import (
    abstractmethod,
)
from dataclasses import (
    dataclass,
)
from typing import (
    List,
)


UNSET_LABEL = 'n.a.'
UNSET_NUMBR = -1
INVALID_UNSET_LABEL = 'UNSET'

LABEL_VALIDATOR_SCAN_COMBINED = 'ScanCombinedValidator'
LABEL_VALIDATOR_SCAN_CHANNEL = 'ScanMetadataChannel'
LABEL_VALIDATOR_SCAN_COMPRESSION = 'ScanMetadataCompression'
LABEL_VALIDATOR_SCAN_RESOLUTION = 'ScanMetadataResolution'
LABEL_VALIDATOR_SCAN_FILEDATA = 'ScanFiledata'
LABEL_VALIDATOR_SCAN_PHOTOMETRICS = 'ScanPhotometrics'


@dataclass
class Invalid:
    """Container for invalid data"""

    label: str
    info: str


class Validator:
    """Common Base Interface"""

    def __init__(self, label: str, input_data):
        self.label = label
        self.input_data = input_data
        self.invalids: List[Invalid] = []

    @abstractmethod
    def valid(self) -> bool:
        """Specific implementation is subject 
        of concrete Validator Implementation
        """

        return len(self.invalids) == 0
