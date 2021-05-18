#!/bin/bash

#start producer
cd ${APP_PATH} && java -cp ${APPLICATION_JAR} ${KAFKA_CLASS} 60
#60 is publishing speed, meaning that 1 minute of timestamps is converted to 1 second in real time