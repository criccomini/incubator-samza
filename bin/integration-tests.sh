#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$DIR/..
# always use absolute paths for TEST_DIR
TEST_DIR=$(cd $(dirname $1); pwd)/$(basename $1)
SCRIPTS_DIR=$TEST_DIR/scripts

if test -z "$TEST_DIR"; then
  echo
  echo "  USAGE:"
  echo
  echo "    ${BASH_SOURCE[0]##*/} \"<dirname to run tests in>\""
  echo
  exit 0
fi

# safety check for virtualenv
if [ -f $HOME/.pydistutils.cfg ]; then
  echo "Virtualenv can't run while $HOME/.pydistutils.cfg exists."
  echo "Please remove $HOME/.pydistutils.cfg, and try again."
  exit 0
fi

# build integration test tarball
./gradlew releaseTestJobs

# create integration test directory
mkdir -p $TEST_DIR
rm -rf $SCRIPTS_DIR
cp -r samza-test/src/main/python/ $SCRIPTS_DIR
cp ./samza-test/build/distributions/samza-test*.tgz $TEST_DIR
cd $TEST_DIR

# setup virtualenv locally if it's not already there
VIRTUAL_ENV=virtualenv-12.0.2
if [[ ! -d "${TEST_DIR}/${VIRTUAL_ENV}" ]] ; then
  curl -O https://pypi.python.org/packages/source/v/virtualenv/$VIRTUAL_ENV.tar.gz
  tar xvfz $VIRTUAL_ENV.tar.gz
fi

# build a clean virtual environment
SAMZA_INTEGRATION_TESTS_DIR=$TEST_DIR/samza-integration-tests
if [[ ! -d "${SAMZA_INTEGRATION_TESTS_DIR}" ]] ; then
  python $VIRTUAL_ENV/virtualenv.py $SAMZA_INTEGRATION_TESTS_DIR
fi

# activate the virtual environment
source $SAMZA_INTEGRATION_TESTS_DIR/bin/activate

# install zopkio and requests
pip install -r $SCRIPTS_DIR/requirements.txt

# run the tests
zopkio --config-overrides remote_install_path=$TEST_DIR --console-log-level INFO --nopassword $SCRIPTS_DIR/tests.py

# go back to execution directory
deactivate
cd $DIR
