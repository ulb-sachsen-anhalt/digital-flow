# -*- coding: utf-8 -*-

import os
import subprocess
import time

from unittest import (
    mock,
)

import pytest

from digiflow import (
    DerivansManager,
    run_profiled,
    id_generator,
    ContainerDerivansManager,
    DEFAULT_DERIVANS_IMAGE,
)


_DUMMY_METS = '<xml/>'


def test_generator_with_prefix_and_suffix():
    """
    Generate values with prefix and suffix
    """

    generator = id_generator(start=42, prefix='FOO_', suffix='_BAR')
    assert next(generator) == 'FOO_0043_BAR'
    assert next(generator) == 'FOO_0044_BAR'


def test_generator_with_mixed_previous_value():
    """
    Generate next value from previous value
    Guess prefix and where numerical part starts
    """

    generator = id_generator(previous_value="phys737438")
    assert next(generator) == 'phys737439'


def test_generator_with_numerical_start_and_padded_value():
    """
    Generate next value from start with padded to 4 digits
    """

    generator = id_generator(start=126, padded=4)
    assert next(generator) == '0127'


# pylint: disable=missing-function-docstring
@run_profiled
def my_func():
    time.sleep(.2)
    return 42


@run_profiled
def my_func_void():
    time.sleep(.2)


@run_profiled
def my_func_arg(rang):
    time.sleep(rang)
    return 27


@run_profiled
def my_func_args(aaa, bbb):
    time.sleep(.25)
    return aaa + bbb


# pylint: disable=missing-class-docstring
class MyClass:

    def __init__(self):
        self.val = 2

    @run_profiled
    def my_func_args(self, arg1, arg2):
        time.sleep(.25)
        return self.val + (arg1 * arg2)

    @staticmethod
    @run_profiled
    def my_func_static(arg1, arg2):
        time.sleep(.25)
        return arg1 * arg2


def test_utils_profile_method_no_arg():
    assert my_func() == (0.20, 'my_func', 42)


def test_utils_profile_arg():
    assert my_func_arg(0.75) == (0.75, 'my_func_arg', 27)


def test_utils_profile_noargs_noreturns():
    assert my_func_void() == (0.20, 'my_func_void', None)


def test_utils_profile_args_return():
    assert my_func_args(3, 5) == (0.25, 'my_func_args', 8)


def test_utils_profile_class_context():
    my_stuff = MyClass()
    assert my_stuff.my_func_args(3, 5) == (0.25, 'MyClass.my_func_args', 17)
    assert \
        MyClass.my_func_static(3, 5) == (0.25, 'MyClass.my_func_static', 15)


@mock.patch('docker.models.images.ImageCollection.pull')
def test_derivans_containermanager(mock_pull, tmp_path):
    """Check init phase of containerized
    Derivans execution context tries to
    pull required default container image"""

    # arrange
    test_project_root = tmp_path / 'migrationtest'
    test_project_root.mkdir()
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)
    dmanager = ContainerDerivansManager(mets_file)

    # act
    dmanager.init()

    # assert
    assert mock_pull.call_count == 1
    call_args = mock_pull.call_args.args
    assert len(call_args) == 2
    image_label = DEFAULT_DERIVANS_IMAGE.split(':', maxsplit=1)[0]
    assert call_args[0] == f'{image_label}'
    assert call_args[1] == 'latest'


@mock.patch("docker.models.containers.ContainerCollection.run")
@mock.patch('docker.models.images.ImageCollection.pull')
def test_derivans_containermanager_additional_args(mock_pull, mock_run, tmp_path):
    """Check init phase of containerized
    Derivans execution context tries to
    pull required default container image"""

    # arrange
    test_project_root = tmp_path / 'migrationtest'
    test_project_root.mkdir()
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)
    dmanager = ContainerDerivansManager(mets_file)
    dmanager.additional_args = "-f /data/conf/my_footer.png"

    # act
    dmanager.init()
    dmanager.start()

    # assert
    assert mock_pull.call_count == 1
    assert mock_run.call_count == 1
    assert dmanager.run_command.endswith(" -f /data/conf/my_footer.png")


def test_derivans_manager_with_path_bin_dir(tmp_path):
    """
    Raise Exception because no valid Maven project
    Directory exists but required to build Derivans
    """

    # arrange
    test_project_root = tmp_path / 'migrationtest'
    the_derivans = test_project_root / 'digital-derivans' / 'target'
    test_project_root.mkdir()
    os.makedirs(str(the_derivans), exist_ok=True)
    test_project_bin = test_project_root / 'bin'
    test_project_bin.mkdir()
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)

    path_mvn_project = str(test_project_root / 'digital-derivans')
    dmanager = DerivansManager(
        mets_file, path_binary=str(test_project_bin),
        path_mvn_project=path_mvn_project)
    cwd = os.getcwd()

    # act
    with pytest.raises(subprocess.CalledProcessError) as err:
        dmanager.init()
    os.chdir(cwd)  # subprocess is changing the cwd, so lets go back

    # assert
    assert ("Command 'mvn clean package -DskipTests' "
            "returned non-zero exit status") in str(err.value)


def test_derivans_manager_none_path_binary(tmp_path):
    """Exception must be thrown if invalid
    None-path to local binary provided"""

    # arrange
    test_project_root = tmp_path / 'migrationtest'
    test_project_root.mkdir()
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)

    # act
    with pytest.raises(RuntimeError) as exc:
        DerivansManager(mets_file, path_binary=None)

    # assert
    assert 'invalid path_binary' in str(exc.value)


def _forward_derivans_call(*args):
    """take some rest and return alike
    the *real* call would do"""

    _delay = args[0]
    time.sleep(_delay)
    yield (_delay, 'execute', 'result')


@mock.patch('digiflow.DerivansManager._execute_derivans')
@mock.patch('digiflow.DerivansManager._identify_derivans_bin')
def test_derivans_start_set_exec(mock_check, mock_call, tmp_path):
    # arrange
    test_project_root = tmp_path / 'migrationtest'
    the_derivans = test_project_root / 'digital-derivans' / 'target'
    test_project_root.mkdir()
    os.makedirs(str(the_derivans), exist_ok=True)
    test_project_bin = test_project_root / 'bin'
    test_project_bin.mkdir()

    # create some dummy file
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)
    path_mvn_project = str(test_project_root / 'digital-derivans')
    dmanager = DerivansManager(
        mets_file, path_binary=str(test_project_bin),
        path_mvn_project=path_mvn_project)
    # here is the important detail
    dmanager.path_exec = '/usr/lib/jvm/java-11-openjdk-amd64/bin/java'
    mock_check.return_value = str(test_project_bin)
    mock_call.side_effect = _forward_derivans_call(0.1)

    # act
    dmanager.init()
    result = dmanager.start()

    # assert
    assert mock_call.call_count == 1
    assert result.command.startswith('/usr/lib/jvm/java-11-openjdk-amd64/bin/java -jar')


@mock.patch('digiflow.DerivansManager._execute_derivans')
@mock.patch('digiflow.DerivansManager._identify_derivans_bin')
def test_derivans_start_default(mock_check, mock_call, tmp_path):
    # arrange
    test_project_root = tmp_path / 'migrationtest'
    the_derivans = test_project_root / 'digital-derivans' / 'target'
    test_project_root.mkdir()
    os.makedirs(str(the_derivans), exist_ok=True)
    test_project_bin = test_project_root / 'bin'
    test_project_bin.mkdir()

    # create some dummy file
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)
    path_mvn_project = str(test_project_root / 'digital-derivans')
    dmanager = DerivansManager(
        mets_file, path_binary=str(test_project_bin),
        path_mvn_project=path_mvn_project)
    mock_check.return_value = str(test_project_bin)
    mock_call.side_effect = _forward_derivans_call(0.1)

    # act
    dmanager.init()
    result = dmanager.start()

    # assert
    assert mock_call.call_count == 1
    assert result.command.startswith('java -jar')


@mock.patch('digiflow.DerivansManager._execute_derivans')
@mock.patch('digiflow.DerivansManager._identify_derivans_bin')
def test_derivans_start_with_additional_config(mock_check, mock_call, tmp_path):
    """Prevent Regression and catch bug: 
        [undefined]TypeError: bad operand type for unary +: 'str'
    """

    # arrange
    test_project_root = tmp_path / 'migrationtest'
    the_derivans = test_project_root / 'digital-derivans' / 'target'
    test_project_root.mkdir()
    os.makedirs(str(the_derivans), exist_ok=True)
    test_project_bin = test_project_root / 'bin'
    test_project_bin.mkdir()

    # create some dummy file
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)
    path_mvn_project = str(test_project_root / 'digital-derivans')
    dmanager = DerivansManager(
        mets_file, path_binary=str(test_project_bin),
        path_mvn_project=path_mvn_project)
    mock_check.return_value = str(test_project_bin)
    dmanager.path_configuration = '/path/to/derivans.ini'
    mock_call.side_effect = _forward_derivans_call(0.1)

    # act
    dmanager.init()
    result = dmanager.start()

    # assert
    assert mock_call.call_count == 1
    assert result.command.startswith('java -jar')
    assert result.command.endswith('/path/to/derivans.ini')


@mock.patch('digiflow.DerivansManager._execute_derivans')
@mock.patch('digiflow.DerivansManager._identify_derivans_bin')
def test_derivans_start_with_java_xargs(mock_check, mock_call, tmp_path):
    """Prevent Regression and catch bug: 
        X11 connection rejected because of wrong authentication.
Exception in thread "main" java.awt.AWTError: Can't connect to X11 window server using 'localhost:11.0' as the value of the DISPLAY variable.
        at java.desktop/sun.awt.X11GraphicsEnvironment.initDisplay(Native Method)
        at java.desktop/sun.awt.X11GraphicsEnvironment$1.run(X11GraphicsEnvironment.java:102)
        at java.base/java.security.AccessController.doPrivileged(Native Method)
        at java.desktop/sun.awt.X11GraphicsEnvironment.<clinit>(X11GraphicsEnvironment.java:61)
        at java.base/java.lang.Class.forName0(Native Method)
        at java.base/java.lang.Class.forName(Class.java:315)
        at java.desktop/java.awt.GraphicsEnvironment$LocalGE.createGE(GraphicsEnvironment.java:101)
        at java.desktop/java.awt.GraphicsEnvironment$LocalGE.<clinit>(GraphicsEnvironment.java:83)
        at java.desktop/java.awt.GraphicsEnvironment.getLocalGraphicsEnvironment(GraphicsEnvironment.java:129)
        at de.ulb.digital.derivans.derivate.FontHandler.forGraphics(FontHandler.java:37)

        solved by passing "-Djava.awt.headless=true" to java call
    """

    # arrange
    test_project_root = tmp_path / 'migrationtest'
    the_derivans = test_project_root / 'digital-derivans' / 'target'
    test_project_root.mkdir()
    os.makedirs(str(the_derivans), exist_ok=True)
    test_project_bin = test_project_root / 'bin'
    test_project_bin.mkdir()

    # create some dummy file
    mets_file = os.path.join(str(test_project_root), 'mets_mods.xml')
    with open(mets_file, 'w', encoding='utf-8') as fh_mets:
        fh_mets.write(_DUMMY_METS)
    path_mvn_project = str(test_project_root / 'digital-derivans')
    dmanager = DerivansManager(
        mets_file, path_binary=str(test_project_bin),
        path_mvn_project=path_mvn_project)
    mock_check.return_value = str(test_project_bin)
    dmanager.path_configuration = '/path/to/derivans.ini'
    dmanager.additional_args = '-Djava.awt.headless=true'
    mock_call.side_effect = _forward_derivans_call(0.1)

    # act
    dmanager.init()
    result = dmanager.start()

    # assert
    assert mock_call.call_count == 1
    assert ' -Djava.awt.headless=true' in result.command
