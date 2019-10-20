FROM python:3.7.4-alpine3.10

MAINTAINER TAC <tac@tac42.net>
# Forward JSON message from RabbitMQ to Clickhouse

RUN mkdir /grebe
ADD README.md grebe.py requirements.txt /grebe/
ADD br2dl /grebe/br2dl
WORKDIR /grebe

RUN apk add --no-cache --virtual .build-deps gcc musl-dev && \
    pip install -r requirements.txt && \
    apk del .build-deps gcc musl-dev binutils gmp libgomp libatomic libgcc mpfr3 mpc1 libstdc++ musl-dev

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

# File path to specified column types
ENV TYPE_FILE /type.yml

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
    --type-file ${TYPE_FILE}\
    --log-level ${LOG_LEVEL} \
    --log-file /logs/`cat /etc/hostname`/${LOG_FILE} \
    --log-format "${LOG_FORMAT}" \
    --log-file-count ${LOG_FILE_COUNT}\
    --log-file-size ${LOG_FILE_SIZE}\
    --retry-max-count ${RETRY_MAX_COUNT}
