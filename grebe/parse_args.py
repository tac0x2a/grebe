
def parse_args():
    # Argument Parsing
    import argparse
    parser = argparse.ArgumentParser(description='Forward Data-like string message from RabbitMQ to Clickhouse')
    parser.add_argument('queue_name', help='Queue name to subscribe on RabbitMQ')  # Required
    parser.add_argument('-mh', help='RabbitMQ host', default='localhost')
    parser.add_argument('-mp', help='RabbitMQ port', type=int, default=5672)
    parser.add_argument('-dh', help='Clickhouse host', default='localhost')
    parser.add_argument('-dp', help='Clickhouse port by native connection', type=int, default=9000)

    parser.add_argument('--schema-store', help='Schema store location', choices=['local', 'rdb'], default="local")
    parser.add_argument('--local-schema-dir', help='Schema DB directory path when schema-sotre is local', default="schemas")

    parser.add_argument('--type-file', help='File path to specified column types')
    parser.add_argument('--tz', help='Timezone string will be used as default offset in parsing source string if it has no offset')

    parser.add_argument('--api-port', help='Port number of grebe Web API. It is disabled if this is not provided.', type=int, default=None)

    parser.add_argument('--log-level', help='Log level', choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default='INFO')
    parser.add_argument('--log-format', help='Log format by \'logging\' package', default='[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s')  # Optional

    parser.add_argument('--log-file', help='Log file path')
    parser.add_argument('--log-file-count', help='Log file keep count', type=int, default=1000)
    parser.add_argument('--log-file-size', help='Size of each log file', type=int, default=1000000)  # default 1MB

    parser.add_argument('--retry-max-count', help='Max count of retry to processing. Message is discard when exceeded max count.', type=int, default=3)

    return parser.parse_args()
