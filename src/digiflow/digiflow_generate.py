# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import platform
import re
import subprocess
import time
import typing

from abc import abstractmethod, ABC
from dataclasses import dataclass
from pathlib import Path

import docker
from docker.models.containers import Container
from docker.types import Mount

# default sub dir for structure creation
DEFAULT_STRUCTURE_DIR = "MAX"

# derivates
DEFAULT_DERIVANS_IMAGE = "ghcr.io/ulb-sachsen-anhalt/digital-derivans:latest"
DEFAULT_DERIVANS_TIMEOUT = 10800
DERIVANS_LABEL = "derivans"
DERIVANS_CNT_DATA_DIR: typing.Final[str] = "/usr/derivans/data"
DERIVANS_CNT_CONF_DIR: typing.Final[str] = "/usr/derivans/config"
DERIVANS_CNT_LOGG_DIR: typing.Final[str] = "/usr/derivans/log"


def id_generator(start=0, prefix=None, suffix=None, previous_value=None, padded=4):
    """
    Generate Numbers with schema [<prefix>]<4digits>[<suffix>]
    with default padded zeros to match 4-digit-numbers
    or
    Try to guess prefix from given previous_value
    """

    _prefix = None
    _start = None
    if previous_value is not None:
        i = 0
        while i < len(previous_value):
            if not _prefix and previous_value[i].isnumeric():
                _prefix = previous_value[:i]
                _start = previous_value[i:]
            i += 1

    if _prefix and not prefix:
        prefix = _prefix
    if _start and not start:
        start = int(_start)

    for num in range(start + 1, 99999999):
        number = str(num).zfill(padded)
        if prefix:
            number = f"{prefix}{number}"
        if suffix:
            number = f"{number}{suffix}"
        yield number


_T = typing.TypeVar("_T")
_FuncWrapperResult = typing.Tuple[float, str, _T]
RunProfiledResult = typing.Callable[..., _FuncWrapperResult[_T]]


def run_profiled(func: typing.Callable) -> RunProfiledResult:
    """
    Decorator to profile method execution time
    in seconds as float with 2 decimal digits
    """

    def _get_func_name(_func: typing.Callable) -> str:
        _label = str(_func)
        match = re.match(r".*function ([\w.]+) *", _label)
        if match:
            return match.group(1)
        else:
            # this recognizes even MagicMock-Objects
            match = re.match(r".*name='([a-z_]+)'.*", _label)
            if match:
                return match.group(1)
        return "func"

    def func_wrapper(*args) -> _FuncWrapperResult[_T]:
        result = None
        start: float = time.perf_counter()
        if args:
            result = func(*args)
        else:
            result = func()
        delta: float = time.perf_counter() - start

        # name of callee
        label: str = _get_func_name(func)
        return round(delta, 2), label, result

    return func_wrapper


@run_profiled
def run_command(cmd, timeout) -> subprocess.CompletedProcess:
    """Forward command with given timeout"""
    return subprocess.run(
        cmd,
        shell=True,
        check=True,
        capture_output=True,
        encoding="UTF-8",
        timeout=timeout,
    )


class DerivansManagerError(RuntimeError):
    """DerivansManager related errors"""


@dataclass(frozen=True)
class DerivansResult:
    """Encapsulate Derivans outcome"""

    command: str
    duration: float
    result: typing.Optional[ContainerProcResult] = None
    label: typing.Optional[str] = None


@dataclass(frozen=True)
class ContainerProcResult:
    """Encapsulate container process outcome"""
    exit_code: int
    logs: str


class BaseDerivansManager(ABC):
    """Manage Derivans component calls
    from within python modules
    """

    @staticmethod
    def create(
        path_input: Path,
        path_configuration: typing.Optional[Path] = None,
        path_logging: typing.Optional[Path] = None,
        container_image_name: typing.Optional[str] = None,
        path_binary: typing.Optional[Path] = None,
    ) -> BaseDerivansManager:
        """Create actual DerivansManager instance
        depending on provided parameters"""

        if container_image_name is not None:
            return ContainerDerivansManager(
                path_mets_file=path_input,
                container_image=container_image_name,
                path_configuration=path_configuration,
                path_logging=path_logging,
            )
        if path_binary is not None:
            return DerivansManager(
                path_mets_file=path_input,
                path_binary=path_binary,
                path_configuration=path_configuration,
            )
        raise DerivansManagerError("container_image_name or path_binary must be provided!")

    def __init__(
        self,
        path_mets_file: Path,
        path_configuration: typing.Optional[Path] = None,
    ):
        if path_configuration and not Path(path_configuration).is_file():
            raise DerivansManagerError(
                f"config missing: {path_configuration}!"
            )
        self.path_mets_file = path_mets_file
        self.path_configuration = path_configuration
        self.additional_args = ""

    @abstractmethod
    def init(self) -> None:
        """Setup application"""

    @abstractmethod
    def start(self) -> DerivansResult:
        """Issue actual derivans instance
        and communicate outcome"""


class DerivansManager(BaseDerivansManager):
    """Local Derivans instance.
    Requires at least recent Java at
    runtime, additionally Maven if
    Derivans must be built at init stage"""

    @property
    def timeout(self):
        """Timeout for derivans workflows"""
        return self._timeout

    @timeout.setter
    def timeout(self, timeout):
        self._timeout = timeout

    def __init__(
        self,
        path_mets_file: Path,
        path_configuration: typing.Optional[Path],
        path_binary: typing.Optional[Path],
    ):
        super().__init__(
            path_mets_file=path_mets_file,
            path_configuration=path_configuration,
        )
        if path_binary is None or not (
            path_binary.is_dir() or path_binary.is_file()
        ):
            raise DerivansManagerError(f"invalid path_binary: {path_binary}!")
        self.path_binary = path_binary
        self._timeout = DEFAULT_DERIVANS_TIMEOUT
        self._label = DERIVANS_LABEL
        self.path_exec = None

    def init(self):
        if isinstance(self.path_binary, Path) and self.path_binary.is_dir():
            _dir = self.path_binary
            self.path_binary = self._identify_derivans_bin()
            if self.path_binary is None:
                raise DerivansManagerError(f"no derivans binary found in {_dir}!")

        # fallback to default 'java' if no need to worry about
        if not self.path_exec:
            self.path_exec = "java"

    def start(self) -> DerivansResult:
        """Create Derivates with provided configuration

        * step into derivans root dir first
        * respect actual operation system due executable path
          wich might contain spaces on windows
        * execute
        * return to previous directory

        """
        assert isinstance(self.path_binary, Path)
        derivans_root = self.path_binary.parent
        prev_dir = os.path.abspath(os.curdir)
        os.chdir(derivans_root)
        path_exec = self.path_exec
        if platform.system() not in ["Linux"]:
            path_exec = f'"{path_exec}"'
        cmd = f"{path_exec} -jar {self.path_binary} {self.path_mets_file}"
        if self.path_configuration:
            cmd += f" -c {self.path_configuration}"
        if self.additional_args:
            cmd += f" {self.additional_args}"
        # disable pylint due it is not able to recognize
        # output being created by decorator
        time_duration, label, result = self._execute_derivans(
            cmd
        )  # pylint: disable=unpacking-non-sequence
        os.chdir(prev_dir)
        return DerivansResult(
            command=cmd,
            result=result,
            duration=time_duration,
            label=label,
        )

    def _identify_derivans_bin(self, the_dir: typing.Optional[Path]=None):
        if not the_dir:
            the_dir = self.path_binary
        if the_dir is None or not the_dir.is_dir():
            raise DerivansManagerError(f"invalid path_binary: {the_dir}!")
        all_files = [f for f in os.listdir(the_dir)
                     if f.endswith(".jar") and self._label in str(f)]
        derivantis = sorted(all_files, reverse=True)
        if len(derivantis) > 0:
            return the_dir / derivantis[0]
        return None

    def _execute_derivans(self, command) -> _FuncWrapperResult:
        return run_command(command, self.timeout)


class ContainerDerivansManager(BaseDerivansManager):
    """Containered Derivans instance.
    Required local container runtime.
    """

    def __init__(
        self,
        path_mets_file: Path,
        path_configuration: typing.Optional[Path],
        path_logging: typing.Optional[Path],
        container_image: str = DEFAULT_DERIVANS_IMAGE,
    ):
        super().__init__(
            path_mets_file=path_mets_file,
            path_configuration=path_configuration,
        )
        self._container_image: str = container_image
        self._client: docker.DockerClient = docker.from_env()
        self._path_logging = path_logging
        self.run_command = ""

    def init(self) -> None:
        repo, tag = self._container_image.split(":")
        if not self._client.images.list(self._container_image):
            self._client.images.pull(repo, tag)

    def start(self) -> DerivansResult:
        mounts: typing.List[Mount] = []
        command: typing.List[str] = []
        mets_path: Path = Path(self.path_mets_file).absolute()
        if mets_path.is_dir():
            command.append(DERIVANS_CNT_DATA_DIR)
            mounts.append(
                Mount(source=str(mets_path), target=DERIVANS_CNT_DATA_DIR, type="bind")
            )
        if mets_path.is_file():
            mets_file_name: str = mets_path.name
            mets_dir: str = str(mets_path.parent.absolute())
            target_mets_file: str = str(
                Path(DERIVANS_CNT_DATA_DIR).joinpath(mets_file_name)
            )
            command.append(target_mets_file)
            mounts.append(
                Mount(source=mets_dir, target=DERIVANS_CNT_DATA_DIR, type="bind")
            )
        if self.path_configuration is not None:
            config_path: typing.Union[Path, None] = Path(self.path_configuration)
            if config_path.exists() and config_path.is_file():
                config_file_name: str = config_path.name
                config_dir: str = str(config_path.parent.absolute())
                target_config_file: str = str(
                    Path(DERIVANS_CNT_CONF_DIR).joinpath(config_file_name)
                )
                mounts.append(
                    Mount(source=config_dir, target=DERIVANS_CNT_CONF_DIR, type="bind")
                )
                command.append("-c")
                command.append(target_config_file)
        if self.additional_args and len(self.additional_args.strip()) > 0:
            command.append(self.additional_args)
        if self._path_logging:
            _log_dir = self._path_logging
            mounts.append(
                Mount(source=str(_log_dir), target=DERIVANS_CNT_LOGG_DIR, type="bind")
            )

        start_time: float = time.perf_counter()
        container: Container = self._client.containers.run(
            image=self._container_image,
            command=command,
            user=os.getuid(),
            mounts=mounts,
            detach=True,
        )
        exit_code: int = container.wait()["StatusCode"]
        logs: str = container.logs().decode("utf-8")
        container.remove()

        full_command_equivalent: typing.List[str] = ["docker run -rm"]
        for mount in mounts:
            full_command_equivalent.append(
                f"--mount type={mount['Type']},source={mount['Source']},target={mount['Target']}"
            )
        full_command_equivalent.append(self._container_image)
        full_command_equivalent.append(" ".join(command))
        dur: float = time.perf_counter() - start_time
        self.run_command = " ".join(full_command_equivalent)
        return DerivansResult(
            command=self.run_command,
            result=ContainerProcResult(exit_code=exit_code, logs=logs),
            duration=dur,
        )
