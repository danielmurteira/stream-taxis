#!/bin/bash

#start producer
cd ${APP_PATH}/
pushd ${APP_PATH}/
python3 -m http.server 9090
popd