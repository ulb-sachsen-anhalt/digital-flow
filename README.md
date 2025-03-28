# digital flow

![Python CI](https://github.com/ulb-sachsen-anhalt/digital-flow/actions/workflows/main.yml/badge.svg)
[![PyPi version](https://badgen.net/pypi/v/digiflow/)](https://pypi.org/project/digiflow/) ![PyPI - Downloads](https://img.shields.io/pypi/dm/digiflow) ![PyPI - License](https://img.shields.io/pypi/l/digiflow) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/digiflow)

Father's little helper for internal library digitalization workflows running on Linux-Systems. Use at own risk.

## Local installation

Create python environment and activate 

```bash
# install via PyPi
python -m pip install digiflow

# run tests
python -m pip install -r tests/test_requirements.txt
python -m pytest
```

## Tests with toxic container

Execute `tox` in a closed environment (and keep track of docker's socket connection):

```bash
#setup_tox.sh your-python-tox-image
docker build --tag <your-test-image> -f Dockerfile.tox .
docker run --rm  -v /var/run/docker.sock:/var/run/docker.sock <yout-test-image> tox
```

## License

This project's source code is licensed under terms of the [MIT license](https://opensource.org/licenses/MIT).

NOTE: This project depends on components that may use different license terms.
