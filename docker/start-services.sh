#!/bin/bash

#store files in app path
cp /usr/src/app/target/${APPLICATION_JAR} ${APP_PATH}
cp /usr/src/app/src/main/resources/*.gz ${APP_PATH}
cp /usr/src/app/src/main/frontend/*.html ${APP_PATH}
cp /usr/local/bin/node_modules/leaflet-arrowheads/src/leaflet*.js ${APP_PATH}

#run zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
echo "Started Zookeeper..."

sleep 5

#run kafka server
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
echo "Started Kafka..."

sleep 5

#create empty maps to show something until data starts getting consumed
cp ${APP_PATH}/Map.html ${APP_PATH}/FreqRoutes.html
cp ${APP_PATH}/Map.html ${APP_PATH}/ProfitAreas.html
#share maps
sh ${APP_PATH}/share-maps.sh &
echo "Sharing visualization maps..."
echo "First map available at http://localhost:9090/FreqRoutes.html..."
echo "Second map available at http://localhost:9090/ProfitAreas.html..."

sleep 5

#merge input file parts
echo "Merging resource files..."
cd ${APP_PATH} && cat sorted_data_* > sorted_data.csv.gz && rm sorted_data_*
#start producer
echo "Starting producer..."
sh ${APP_PATH}/start-producer.sh &

sleep 5

#start consumer
echo "Starting consumer...                    "
sh ${APP_PATH}/start-consumer.sh