FROM bde2020/spark-submit:2.4.5-hadoop2.7

LABEL maintainer="Daniel Murteira <domurteira@gmail.com>"

ENV KAFKA_HOME /kafka
ENV APP_PATH /taxis-app
ENV APPLICATION_JAR stream-taxis-1.0.0.jar
ENV KAFKA_CLASS com.ddom.taxis.Prod
ENV SPARK_CLASS com.ddom.taxis.Cons

RUN mkdir -p ${KAFKA_HOME}
RUN mkdir -p ${APP_PATH}

#set kafka home
COPY docker/env/kafka_2.11-0.10.2.1/ ${KAFKA_HOME}

#install npm and get leaflet-arrowheads scripts
RUN apk add --update npm && cd /usr/local/bin && npm i leaflet-arrowheads

#compile app
RUN apk add --no-cache openjdk8 maven
RUN mkdir -p /usr/src/app/src
COPY src /usr/src/app/src
COPY pom.xml /usr/src/app
RUN cd /usr/src/app && mvn dependency:resolve
RUN cd /usr/src/app && mvn clean install

#copy scripts
COPY docker/*.sh ${APP_PATH}
RUN chmod +x ${APP_PATH}/*.sh

#expose port for map visualization
EXPOSE 9090

CMD ["/bin/bash", "/taxis-app/start-services.sh"]