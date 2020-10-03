FROM python:3.8.3-slim

LABEL maintainer="tac@tac42.net"
LABEL Description="Grebe is forwarder Data-like string message from RabbitMQ to Clickhouse." Vendor="TAC" Version="1.2.0"

RUN mkdir /grebe
ADD README.md grebe.py requirements.txt /grebe/
ADD grebe /grebe/grebe
WORKDIR /grebe

RUN apt-get update && apt-get -y upgrade \
    && apt-get install -y --no-install-recommends gcc g++ musl-dev \
    && pip install -r requirements.txt \
    && apt-get clean \
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /tmp/* /var/tmp/* \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/lib/apt/lists/*


################
# Environments #
################
# Queue name to subscribe on RabbitMQ
ENV MQ_QNAME ''


# RabbitMQ host
ENV MQ_HOST  ''

# RabbitMQ port
ENV MQ_PORT  5672

# Clickhouse host
ENV DB_HOST  ''

# Clickhouse port by native connection
ENV DB_PORT  9000

# Schema store location
ENV SCHEMA_STORE local

# Path to meta data store as local file. If this parameter skipped, meta data will be create on DB
ENV LOCAL_METASTORE_FILE ""

# Timezone string will be used as default offset in parsing source string if it has no offset
ENV TZ_STR "UTC"

# Port number of grebe Web API. It is disabled if this value is 0 or not provided.
ENV API_PORT 0

# Schema DB directory path when schema-sotre is local
ENV LOCAL_SCHEMA_DIR /schemas

# Log level [DEBUG, INFO, WARN, ERROR]
ENV LOG_LEVEL INFO

#  Log format by 'logging' package
ENV LOG_FORMAT "[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s"

# Log file name
ENV LOG_FILE greabe.log

# Count of file files kept
ENV LOG_FILE_COUNT 1000

# Size of each log file
ENV LOG_FILE_SIZE  1000000

# Max count of retry to processing messge
ENV RETRY_MAX_COUNT 3


RUN mkdir /logs /schemas
VOLUME /logs
VOLUME /schemas

ENTRYPOINT python ./grebe.py ${MQ_QNAME} \
    -mh ${MQ_HOST} -mp ${MQ_PORT} \
    -dh ${DB_HOST} -dp ${DB_PORT} \
    --schema-store ${SCHEMA_STORE}\
    --local-schema-dir ${LOCAL_SCHEMA_DIR}\
    --local-meta-store-file ${LOCAL_METASTORE_FILE}\
    --tz "${TZ_STR}" \
    --api-port ${API_PORT} \
    --log-level ${LOG_LEVEL} \
    --log-file /logs/`cat /etc/hostname`/${LOG_FILE} \
    --log-format "${LOG_FORMAT}" \
    --log-file-count ${LOG_FILE_COUNT}\
    --log-file-size ${LOG_FILE_SIZE}\
    --retry-max-count ${RETRY_MAX_COUNT}
