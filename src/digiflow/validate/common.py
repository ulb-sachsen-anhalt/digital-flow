"""Common Validation API"""

import abc
import dataclasses
import datetime
import pathlib
import typing


UNSET_NUMBR = -1
INVALID_LABEL_UNSET = "INVALID_UNSET"
INVALID_LABEL_RANGE = "INVALID_RANGE"
INVALID_LABEL_TYPE = "INVALID_TYPE"

DATETIME_SRC_FORMAT = "%Y:%m:%d %H:%M:%S"


@dataclasses.dataclass
class Invalid:
    """Container for invalid local data"""

    label: str
    location: pathlib.Path
    info: str


class InputFile(abc.ABC):
    """Base class for local validation input data"""

    def __init__(self, input_path: typing.Any):
        self.input_path = input_path
        self.file_size = 0
        self.time_stamp = None
        self.check_sum_512 = None
        dt_object = datetime.datetime.now()
        self.time_stamp = dt_object.strftime(DATETIME_SRC_FORMAT)


class Validator(abc.ABC):
    """Common Base Interface to validate local data"""

    def __init__(self, input_data: typing.Optional[InputFile], **kwargs):
        self.kwargs = kwargs
        self.input_file: typing.Optional[InputFile] = input_data
        self.invalids: typing.List[Invalid] = []
        self.set_data(input_data)

    def is_valid(self) -> bool:
        """Check if validation passed without errors"""
        self.check()
        return len(self.invalids) == 0

    @abc.abstractmethod
    def check(self) -> None:
        """Specific implementation is subject
        of concrete Validator Implementation
        """

    @abc.abstractmethod
    def set_data(self, input_data: typing.Optional[InputFile]) -> None:
        """Set or update input data for validation"""

    @property
    def label(self) -> str:
        """Label for this validator instance"""
        raise NotImplementedError("Subclasses must implement label property")


class ValidatorFactory(abc.ABC):
    """Factory for creating and managing validator instances

    Supports configuration management and runtime registration of validators.
    """

    _registry: typing.ClassVar[typing.Dict[str, typing.Type[Validator]]] = {
    }

    def __init__(self, config: typing.Optional[typing.Any] = None):
        """Initialize factory with configuration

        Args:
            config: Configuration to use. Creates default if None.
        """
        self.config = config

    @classmethod
    def register(
        cls, validator_label: str, validator_class: typing.Type[Validator]
    ) -> None:
        """Register a validator class

        Args:
            validator_label: Unique identifier for the validator
            validator_class: Class that inherits from Validator

        Raises:
            TypeError: If validator_class doesn't inherit from Validator
        """
        if not issubclass(validator_class, Validator):
            raise TypeError(f"{validator_class} must inherit from Validator")
        cls._registry[validator_label] = validator_class

    @classmethod
    def unregister(cls, validator_label: str) -> None:
        """Remove a validator from the registry"""
        cls._registry.pop(validator_label, None)

    @classmethod
    def get_class(cls, validator_label: str) -> typing.Type[Validator]:
        """Get validator class for label

        Args:
            validator_label: Label of the validator to retrieve

        Returns:
            The validator class

        Raises:
            KeyError: If validator_label not found
        """
        if validator_label not in cls._registry:
            available = ", ".join(cls._registry.keys())
            raise KeyError(
                f"No validator registered for '{validator_label}'. "
                f"Available validators: {available}"
            )
        return cls._registry[validator_label]

    @classmethod
    def get(cls, validator_label: str) -> typing.Type[Validator]:
        """Get validator class for label (legacy method for backward compatibility)

        Args:
            validator_label: Label of the validator to retrieve

        Returns:
            The validator class

        Raises:
            NotImplementedError: If validator_label not found
        """
        if validator_label not in cls._registry:
            raise NotImplementedError(f"No implementation for {validator_label}!")
        return cls._registry[validator_label]

    def create(
        self,
        validator_label: str,
        input_data: typing.Any,
        override_config: typing.Optional[dict] = None,
        **kwargs,
    ) -> Validator:
        """Create a validator instance with configuration

        Args:
            validator_label: Validator to create
            input_data: Input for validator
            override_config: Config overrides for this instance only
            **kwargs: Additional kwargs (for backward compatibility)

        Returns:
            Configured validator instance
        """
        validator_class = self.get_class(validator_label)

        # Merge default config with overrides and kwargs
        assert self.config is not None, "Factory config must be set to create validators"
        config_dict = self.config.to_dict()
        if override_config:
            config_dict.update(override_config)
        if kwargs:
            config_dict.update(kwargs)

        return validator_class(input_data, **config_dict)

    def update_config(self, **kwargs) -> None:
        """Update factory configuration

        Args:
            **kwargs: Configuration parameters to update
        """
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

    @classmethod
    def list_validators(cls) -> typing.List[str]:
        """Get list of all registered validator labels"""
        return list(cls._registry.keys())

    @classmethod
    def has_validator(cls, validator_label: str) -> bool:
        """Check if a validator is registered"""
        return validator_label in cls._registry

    @property
    def validators(self) -> typing.Dict[str, typing.Type[Validator]]:
        """Property for backward compatibility with old code"""
        return self._registry
