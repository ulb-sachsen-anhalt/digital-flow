# digital flow

[![pipeline status](https://git.itz.uni-halle.de/ulb/ulb-digiflow/badges/master/pipeline.svg)](https://git.itz.uni-halle.de/ulb/ulb-digiflow/badges/master/pipeline.svg)
[![coverage report](https://git.itz.uni-halle.de/ulb/ulb-digiflow/badges/master/coverage.svg)](https://git.itz.uni-halle.de/ulb/ulb-digiflow/commits/master)

Father's little helper for internal library digitalization workflows running on Linux-Systems.

## Local tests using Tox on Ubuntu 20.04 LTS

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

This executes `tox` in a closed container without creating distributions.
