#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$DIR/..
TEST_DIR=$1

if test -z "$TEST_DIR"; then
  echo
  echo "  USAGE:"
  echo
  echo "    ${BASH_SOURCE[0]##*/} \"<dirname to run tests in>\""
  echo
  exit 0
fi

# build integration test tarball
./gradlew clean releaseTestJobs

# create integration test directory
mkdir -p $TEST_DIR
cp ./samza-test-jobs/build/distributions/samza-test-jobs-*.tgz $TEST_DIR
cd $TEST_DIR

# setup virtualenv locally if it's not already there
VIRTUAL_ENV=virtualenv-1.9
if [[ ! -d "${TEST_DIR}/${VIRTUAL_ENV}" ]] ; then
  curl -O https://pypi.python.org/packages/source/v/virtualenv/$VIRTUAL_ENV.tar.gz
  tar xvfz $VIRTUAL_ENV.tar.gz
fi

# build a clean virtual environment
rm -rf samza-integration-tests
python $VIRTUAL_ENV/virtualenv.py samza-integration-tests
cd $TEST_DIR/samza-integration-tests

# activate the virtual environment
source bin/activate

# go back to execution directory
deactivate
cd $DIR
