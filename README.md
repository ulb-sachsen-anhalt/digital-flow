# digital flow

![Python CI](https://github.com/ulb-sachsen-anhalt/digital-flow/actions/workflows/main.yml/badge.svg)
[![PyPi version](https://badgen.net/pypi/v/digiflow/)](https://pypi.org/project/digiflow/) ![PyPI - Downloads](https://img.shields.io/pypi/dm/digiflow) ![PyPI - License](https://img.shields.io/pypi/l/digiflow) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/digiflow)

Father's little helper for internal library digitalization workflows running on Linux-Systems.

## Testing

Testing with tox uses different Python versions.

### Local tests using Tox on Ubuntu 20.04 LTS

For running tox locally for different Python Versions, it is required to have them installed

```bash
apt update
apt-get -y install software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt update
apt-get -y install python python3.6 python3-pip python-setuptools

# activate project's environment
. ./venv/bin/activate
pip install tox
tox
```

### Run Tests with toxic containers

```bash
setup_tox.sh your-python-tox-image
docker build --tag <your-test-image> --build-arg BASE_IMAGE=<your-python-tox-image> -f Dockerfile.tox .
docker run --rm <your-test-image> tox
```

This executes `tox` in a closed container without distributioning.
