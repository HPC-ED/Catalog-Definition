#!/bin/bash -x
####################
# MacOS
####################

# Install dependencies using Homebrew, Mac Ports, or other distribution with include/ and lib/::
#  - PostgreSQL
#  - OpenSSL

PYVERSION="3.10"
# Install Python 3.x.x from Mac native packages at python.org
# Using that Python's pip install pipenv, by:
#   /Library/Frameworks/Python.framework/Versions/${PYVERSION}/bin/python${PYVERSION} -m pip install --upgrade pip
#   /Library/Frameworks/Python.framework/Versions/${PYVERSION}/bin/python${PYVERSION} -m pip install pipenv
PGBASE="/opt/homebrew/Cellar/postgresql@14/14.9/"
SSLBASE="/opt/homebrew/Cellar/openssl@1.1/1.1.1w/"

env PATH="${PGBASE}/bin:${PATH}" LDFLAGS="-I${PGBASE}/include -L${PGBASE}/lib -L${SSLBASE}/lib" pipenv --python `which python${PYVERSION}` --bare install
