#!/bin/bash -

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${SCRIPT_DIR}/../rel/files/

FILE_NAME=antidote.config

cp ${FILE_NAME} ${SCRIPT_DIR}/../dev/dev1/${FILE_NAME}
cp ${FILE_NAME} ${SCRIPT_DIR}/../dev/dev2/${FILE_NAME}
cp ${FILE_NAME} ${SCRIPT_DIR}/../dev/dev3/${FILE_NAME}

cd ${SCRIPT_DIR}/../
./riak_test/bin/antidote-current.sh