# -*- coding: utf-8 -*-
from __future__ import annotations

import itertools
import os
import pathlib
import platform
import re
import shutil
import subprocess
import time
from abc import abstractmethod, ABC
from dataclasses import dataclass

from pathlib import Path

from typing import (
    List,
    Tuple,
    Union,
    Final,
    Callable,
    TypeVar,
)

from PIL import (
    Image
)

import numpy as np

from docker import (
    DockerClient,
    from_env
)
from docker.models.containers import Container
from docker.types import Mount

# default sub dir for structure creation
DEFAULT_STRUCTURE_DIR = 'MAX'

# derivates
DEFAULT_DERIVANS_IMAGE = "ghcr.io/ulb-sachsen-anhalt/digital-derivans:latest"
DEFAULT_DERIVANS_TIMEOUT = 10800
DERIVANS_LABEL = 'derivans'
DERIVANS_CNT_DATA_DIR: Final[str] = "/usr/derivans/data"
DERIVANS_CNT_CONF_DIR: Final[str] = "/usr/derivans/config"
DERIVANS_CNT_LOGG_DIR: Final[str] = "/usr/derivans/log"


def id_generator(
        start=0, prefix=None, suffix=None, previous_value=None, padded=4):
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
            number = "{}{}".format(prefix, number)
        if suffix:
            number = "{}{}".format(number, suffix)
        yield number


def create_grayscale_data(dim):
    arr_floats = np.random.rand(dim[1], dim[0]) * 255
    arr_ints = arr_floats.astype(np.uint8)
    return arr_ints


def create_sample_text():
    return 'some foo text'


class Blueprint():

    def __init__(self,
                 ext='.tif',
                 batch=True,
                 content_func=create_grayscale_data,
                 **kwargs):
        self.ext = ext
        self.batch = batch
        self.content_func = content_func
        self.dimension = None
        self.header = None
        if 'header' in kwargs:
            self.header = kwargs['header']
        self.dimension = kwargs['dimension'] if 'dimension' in kwargs else (
            200, 300)

    def write(self, path):
        if self.ext in ('.tif', '.jpg'):
            self.create_image(path, params=self.header)

    def create_image(self, path_image, params=None):
        """
        Create artificial greyscale images
        """

        data = self.content_func(self.dimension)
        im = Image.fromarray(data)
        store_format = 'TIFF' if self.ext == '.tif' else 'JPEG'
        im.save(path_image, format=store_format, params=params)
        return path_image


class ResourceGenerator:

    def __init__(self, path, blueprint=Blueprint('.jpg'), number=10):
        self.path = path
        self.blueprint = blueprint
        self.content_func = create_sample_text
        self.number = number
        self.start_dir = None

    def get_batch(self, start=0, padded=8):
        paths = self._get_paths(start, self.number, padded)
        aggregated_paths = []
        for path in paths:
            path_dir = self._make_dirs(path)
            path_file = os.path.basename(path)
            real_path = os.path.join(str(path_dir), str(path_file))
            self.blueprint.write(real_path)
            aggregated_paths.append(real_path)
        return aggregated_paths

    def get(self):
        path_dir = self._make_dirs(self.path)
        path_file = os.path.basename(self.path)
        real_path = os.path.join(str(path_dir), str(path_file))
        self.write_single(real_path)
        return real_path

    def write_single(self, path):
        with open(path, 'w') as fhd:
            fhd.write(self.content_func())

    def _make_dirs(self, path):
        the_dir = pathlib.Path(os.path.dirname(path))
        path_dir = os.path.join(str(self.start_dir), str(the_dir))
        os.makedirs(path_dir, exist_ok=True)
        return path_dir

    def _get_paths(self, start, number, padded=8, previous_value=None):
        self.name_generator = id_generator(
            start, previous_value=previous_value, padded=padded)
        names = list(itertools.islice(self.name_generator, number))
        return [os.path.join(self.path, n + self.blueprint.ext) for n in names]


def generate_structure(
        start_dir,
        gens=None,
        number=10
) -> List[str]:

    if gens is None:
        gens = [ResourceGenerator(DEFAULT_STRUCTURE_DIR)]

    """
    Generate test data layouts using a list of Generators for
    * Kitodo2 metadata        (<id>/images/<title>_media/*.tif)
    * Kitodo2 export          (<title>/jpeg/*.jpg)
    * Archivierung            (<title>/content/ ,
                               <title>/metadata/images/<title>_media/*.tif)
    * Migration OAI Workspace (MAX/*.jpg)
    * SAF creation Workspace  (<several-subdirs>)
    """

    generated = []

    if not os.path.isdir(str(start_dir)):
        raise RuntimeError(
            "Invalid start_dir '{}' provided!".format(start_dir))

    for gen in gens:
        gen.start_dir = start_dir
        if gen.blueprint and gen.blueprint.batch:
            resouces = gen.get_batch()
            for res in resouces:
                resource_path = res
                generated.append(resource_path)
        else:
            generated.append(gen.get())

    return generated


_T = TypeVar('_T')
_FuncWrapperResult = Tuple[float, str, _T]
RunProfiledResult = Callable[[], _FuncWrapperResult[_T]]


def run_profiled(func: Callable) -> RunProfiledResult:
    """
    Decorator to profile method execution time
    in seconds as float with 2 decimal digits
    """

    def _get_func_name(_func: Callable) -> str:
        _label = str(_func)
        match = re.match(r'.*function ([\w.]+) *', _label)
        if match:
            return match.group(1)
        else:
            # this recognizes even MagicMock-Objects
            match = re.match(r".*name='([a-z_]+)'.*", _label)
            if match:
                return match.group(1)
        return 'func'

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
    """Forward command with given timeout
    """
    return subprocess.run(
        cmd,
        shell=True,
        check=True,
        capture_output=True,
        encoding='UTF-8',
        timeout=timeout
    )


@dataclass(frozen=True)
class DerivansResult:
    command: str
    duration: float
    result: str | ContainerProcResult | None
    label: str = None


@dataclass(frozen=True)
class ContainerProcResult:
    exit_code: int
    logs: str


class BaseDerivansManager(ABC):
    """Manage Derivans component calls
    from within python modules
    """

    @staticmethod
    def create(
            path_mets_file: str,
            container_image_name: str = None,
            path_binary: str = None,
            path_mvn_project: str = None,
            path_configuration: str = None,
            path_logging: str = None,
    ) -> BaseDerivansManager:
        """Create actual DerivansManager instance
        depending on provided parameters"""

        if container_image_name is not None:
            return ContainerDerivansManager(
                path_mets_file=path_mets_file,
                container_image=container_image_name,
                path_configuration=path_configuration,
                path_logging=path_logging,
            )
        return DerivansManager(
            path_mets_file=path_mets_file,
            path_binary=path_binary,
            path_mvn_project=path_mvn_project,
            path_configuration=path_configuration,
        )

    def __init__(
            self,
            path_mets_file,
            path_configuration=None,
    ):
        if path_configuration and not Path(str(path_configuration)).is_file():
            raise RuntimeError(f"[DerivansManager] config missing: {path_configuration}!")
        self.path_mets_file = path_mets_file
        self.path_configuration = path_configuration

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

    @property
    def label(self):
        return self._label

    @label.setter
    def label(self, label):
        self._label = label

    def __init__(
            self,
            path_mets_file: str,
            path_binary: str,
            path_mvn_project: str = None,
            path_configuration: str = None,
    ):
        if path_binary is None or \
                not (Path(path_binary).is_dir() or Path(path_binary).is_file()):
            raise RuntimeError(f"[DerivansManager] invalid path_binary: {path_binary}!")
        if path_mvn_project is not None \
                and not Path(str(path_mvn_project)).is_dir():
            raise RuntimeError(f"[DerivansManager] invalid path_mvn_project: {path_mvn_project}!")
        super().__init__(
            path_mets_file=path_mets_file,
            path_configuration=path_configuration,
        )
        self.path_binary = path_binary
        self.path_mvn_project = path_mvn_project
        self._timeout = DEFAULT_DERIVANS_TIMEOUT
        self._label = DERIVANS_LABEL
        self.path_exec = None
        self.xargs = ''

    def init(self):
        _path_derivans = self.path_binary

        if Path(self.path_binary).is_dir():
            self.path_binary = self._identify_derivans_bin()

        if self.path_binary is None:
            self._recreate_app(_path_derivans)

        # fallback to default 'java' if no need to worry about
        if not self.path_exec:
            self.path_exec = 'java'

    def start(self) -> DerivansResult:
        """Create Derivates with provided configuration

        * step into derivans root dir first
        * respect actual operation system due executable path
          wich might contain spaces on windows
        * execute
        * return to previous directory

        """
        derivans_root = os.path.dirname(self.path_binary)
        prev_dir = os.path.abspath(os.curdir)
        os.chdir(derivans_root)
        path_exec = self.path_exec
        if platform.system() not in ['Linux']:
            path_exec = f'"{path_exec}"'
        if self.xargs and not self.xargs.startswith(' '):
            self.xargs = ' ' + self.xargs
        cmd = f'{path_exec}{self.xargs} -jar {self.path_binary} {self.path_mets_file}'
        if self.path_configuration:
            cmd += f' -c {self.path_configuration}'
        # disable pylint due it is not able to recognize
        # output being created by decorator
        time_duration, label, result = self._execute_derivans(
            cmd)  # pylint: disable=unpacking-non-sequence
        os.chdir(prev_dir)
        return DerivansResult(
            command=cmd,
            result=result,
            duration=time_duration,
            label=label,
        )

    def _identify_derivans_bin(self, the_dir=None):
        if not the_dir:
            the_dir = self.path_binary
        all_files = [f for f in os.listdir(the_dir) if f.endswith('.jar')]
        derivantis = sorted([f for f in all_files if self._label in str(f)])
        if len(derivantis) > 0:
            return os.path.join(the_dir, derivantis[0])
        return None

    def _recreate_app(self, target_dir):
        dir_derivans = self.path_mvn_project
        if not dir_derivans:
            raise RuntimeError("Derivans project dir unset!")
        if not os.path.isdir(dir_derivans):
            raise RuntimeError(f"Invalid derivans project dir: '{dir_derivans}'!")

        derivans_build_dir = os.path.join(dir_derivans, 'target')
        the_derivans = None
        if os.path.exists(derivans_build_dir):
            the_derivans = self._identify_derivans_bin(derivans_build_dir)

        # if derivans app not build yet, then ...
        if not the_derivans:
            os.chdir(dir_derivans)
            compl_proc = subprocess.run(
                'mvn clean package -DskipTests',
                shell=True,
                check=True,
                timeout=600)
            if compl_proc.returncode != 0:
                raise RuntimeError("Cant build app '%s' in '%s'!")
            the_derivans = self._identify_derivans_bin(derivans_build_dir)

        # copy new built jar to target directory
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
        shutil.copy(the_derivans, target_dir)
        self.path_binary = os.path.join(
            target_dir, os.path.basename(the_derivans))

    def _execute_derivans(self, command) -> _FuncWrapperResult:
        return run_command(command, self.timeout)


class ContainerDerivansManager(BaseDerivansManager):
    """Containered Derivans instance.
    Required local container runtime.
    """

    def __init__(
            self,
            path_mets_file: str,
            container_image: str = DEFAULT_DERIVANS_IMAGE,
            path_configuration: str = None,
            path_logging: str = None,
    ):
        super().__init__(
            path_mets_file=path_mets_file,
            path_configuration=path_configuration,
        )
        self._container_image: str = container_image
        self._client: DockerClient = from_env()
        self._path_logging = path_logging

    def init(self) -> None:
        repo, tag = self._container_image.split(':')
        self._client.images.pull(repo, tag)

    def start(self) -> DerivansResult:
        mounts: List[Mount] = []
        command: List[str] = []
        mets_path: Path = Path(self.path_mets_file).absolute()
        if mets_path.is_dir():
            command.append(DERIVANS_CNT_DATA_DIR)
            mounts.append(Mount(source=str(mets_path), target=DERIVANS_CNT_DATA_DIR, type='bind'))
        if mets_path.is_file():
            mets_file_name: str = mets_path.name
            mets_dir: str = str(mets_path.parent.absolute())
            target_mets_file: str = str(Path(DERIVANS_CNT_DATA_DIR).joinpath(mets_file_name))
            command.append(target_mets_file)
            mounts.append(Mount(source=mets_dir, target=DERIVANS_CNT_DATA_DIR, type='bind'))
        if self.path_configuration is not None:
            config_path: Union[Path, None] = Path(self.path_configuration)
            if config_path.exists() and config_path.is_file():
                config_file_name: str = config_path.name
                config_dir: str = str(config_path.parent.absolute())
                target_config_file: str = str(
                    Path(DERIVANS_CNT_CONF_DIR).joinpath(config_file_name))
                mounts.append(Mount(source=config_dir, target=DERIVANS_CNT_CONF_DIR, type='bind'))
                command.append('-c')
                command.append(target_config_file)
        if self._path_logging:
            _log_dir = self._path_logging
            mounts.append(Mount(source=_log_dir, target=DERIVANS_CNT_LOGG_DIR, type='bind'))

        start_time: float = time.perf_counter()
        container: Container = self._client.containers.run(
            image=self._container_image,
            command=command,
            user=os.getuid(),
            mounts=mounts,
            detach=True
        )
        exit_code: int = container.wait()['StatusCode']
        logs: str = container.logs().decode('utf-8')
        container.remove()

        full_command_equivalent: List[str] = ['docker run -rm']
        for mount in mounts:
            full_command_equivalent.append(
                f"--mount type={mount['Type']},source={mount['Source']},target={mount['Target']}"
            )
        full_command_equivalent.append(self._container_image)
        full_command_equivalent.append(" ".join(command))
        dur: float = time.perf_counter() - start_time
        return DerivansResult(
            command=" ".join(full_command_equivalent),
            result=ContainerProcResult(exit_code=exit_code, logs=logs),
            duration=dur,
        )
