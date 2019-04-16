FROM dockerregistry.reisendeninfo.aws.db.de:5000/base/centos7_6_jdk_1_8:0.1

ENV KAFKA_REQUEST_CLIENT_DIR=/opt/kafka-request-client

COPY  bin/ ${KAFKA_REQUEST_CLIENT_DIR}/bin/
COPY  build/libs ${KAFKA_REQUEST_CLIENT_DIR}/build/libs/
COPY  build/dependant-libs ${KAFKA_REQUEST_CLIENT_DIR}/build/dependant-libs/
COPY  config/ ${KAFKA_REQUEST_CLIENT_DIR}/config/

RUN ln -s ${KAFKA_REQUEST_CLIENT_DIR}/bin/kafka-controlledshutdown /usr/bin/kafka-controlledshutdown && \
  ln -s ${KAFKA_REQUEST_CLIENT_DIR}/bin/kafka-metadata /usr/bin/kafka-metadata && \
  ln -s ${KAFKA_REQUEST_CLIENT_DIR}/bin/kafka-run-class /usr/bin/kafka-run-class

CMD ["tail", "-f", "/dev/null"]