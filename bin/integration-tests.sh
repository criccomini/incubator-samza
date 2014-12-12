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
SAMZA_INTEGRATION_TESTS_DIR=$TEST_DIR/samza-integration-tests
rm -rf $SAMZA_INTEGRATION_TESTS_DIR
python $VIRTUAL_ENV/virtualenv.py $SAMZA_INTEGRATION_TESTS_DIR
cd $SAMZA_INTEGRATION_TESTS_DIR

# activate the virtual environment
source bin/activate

# install zopkio
pip install zopkio

# start an HTTP server to server job TGZ files
HTTP_PID_FILE=$TEST_DIR/http_server.pid
if [[ -a $HTTP_PID_FILE ]] ; then
  kill -9 $(<"$HTTP_PID_FILE")
fi
python -m SimpleHTTPServer &
echo $! > $HTTP_PID_FILE

# run the tests
zopkio $BASE_DIR/samza-test/src/main/python/simple-integration-test.py

# go back to execution directory
deactivate
cd $DIR
