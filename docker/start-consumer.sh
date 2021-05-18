#!/bin/bash

#start consumer
/spark/bin/spark-submit --class ${SPARK_CLASS} ${APP_PATH}/${APPLICATION_JAR}