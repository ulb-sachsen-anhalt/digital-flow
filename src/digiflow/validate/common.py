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
INVALID_LABEL_UNSET = 'INVALID_UNSET'
INVALID_LABEL_RANGE = 'INVALID_RANGE'
INVALID_LABEL_TYPE = 'INVALID_TYPE'


@dataclass
class Invalid:
    """Container for invalid data"""

    label: str
    location: str
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
