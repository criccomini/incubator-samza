#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$DIR/..
TEST_DIR=$1
SCRIPTS_DIR=$TEST_DIR/scripts

if test -z "$TEST_DIR"; then
  echo
  echo "  USAGE:"
  echo
  echo "    ${BASH_SOURCE[0]##*/} \"<dirname to run tests in>\""
  echo
  exit 0
fi

# build integration test tarball
./gradlew releaseTestJobs

# create integration test directory
mkdir -p $TEST_DIR
cp -r samza-test/src/main/python/ $SCRIPTS_DIR
cp ./samza-test-jobs/build/distributions/samza-test-jobs-*.tgz $SCRIPTS_DIR
cd $TEST_DIR

# setup virtualenv locally if it's not already there
VIRTUAL_ENV=virtualenv-1.9
if [[ ! -d "${TEST_DIR}/${VIRTUAL_ENV}" ]] ; then
  curl -O https://pypi.python.org/packages/source/v/virtualenv/$VIRTUAL_ENV.tar.gz
  tar xvfz $VIRTUAL_ENV.tar.gz
fi

# build a clean virtual environment
SAMZA_INTEGRATION_TESTS_DIR=$TEST_DIR/samza-integration-tests
if [[ ! -d "${SAMZA_INTEGRATION_TESTS_DIR}" ]] ; then
  python $VIRTUAL_ENV/virtualenv.py $SAMZA_INTEGRATION_TESTS_DIR
fi
cd $SAMZA_INTEGRATION_TESTS_DIR

# activate the virtual environment
source bin/activate

# install zopkio and requests
pip install -r $BASE_DIR/samza-test/src/main/python/requirements.txt

# run the tests
cd $SCRIPTS_DIR
zopkio --console-log-level INFO --nopassword simple-integration-test.py

# go back to execution directory
deactivate
cd $DIR
