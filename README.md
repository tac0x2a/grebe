# Grebe
![Python application](https://github.com/tac0x2a/grebe/workflows/Python%20application/badge.svg)

```
   　 　 ＿
　　 _,ノ;:｡-:ヽ
　　 ￣｀ヽ　.l|
　　　 　 ﾉ_ノl _,,.,.,..,..,.,..,.,.,,_
　　　　/;;;:ノ:´;'::;'::;'::;'::;'::;'::;'｀＞:"7
  　 ＿l　 "ｰ''""´´""""""´ﾞヾ:/
　　 、 ｀ﾞ''''ーｰ～ｰ--―ー―''"￣￣
　　　　￣　 ―　　＿　　 二　　 ー
```

Grebe is forwarder Data-like string message from RabbitMQ to Clickhouse.

1. Receive JSON, JSON Lines or CSV like string as message from RabbitMQ specified queue on RabbitMQ.
2. Parse, convert, and create table if need it by [Lake Weed](https://github.com/tac0x2a/lake_weed). Each attributes are mapped to columns on Clickhouse table.

## Usage

### Example
```sh
$ pip install -r requirements.txt
$ chmod +x grebe.py

$ ./grebe.py nayco -mh 192.168.11.200 -dh 192.168.11.200
```

## Test
```sh
$ pytest
```

## Help

```sh
$ ./grebe.py -h

usage: grebe.py [-h] [-mh MH] [-mp MP] [-dh DH] [-dp DP]
                [--schema-store {local,rdb}]
                [--local-schema-dir LOCAL_SCHEMA_DIR]
                [--type-file TYPE_FILE]
                [--tz TZ]
                [--api-port API_PORT]
                [--log-level {DEBUG,INFO,WARN,ERROR}]
                [--log-format LOG_FORMAT] [--log-file LOG_FILE]
                [--log-file-count LOG_FILE_COUNT]
                [--log-file-size LOG_FILE_SIZE]
                [--retry-max-count RETRY_MAX_COUNT]
                queue_name

Forward Data-like string message from RabbitMQ to Clickhouse

positional arguments:
  queue_name            Queue name to subscribe on RabbitMQ

optional arguments:
  -h, --help            show this help message and exit
  -mh MH                RabbitMQ host
  -mp MP                RabbitMQ port
  -dh DH                Clickhouse host
  -dp DP                Clickhouse port by native connection
  --schema-store {local,rdb}
                        Schema store location
  --local-schema-dir LOCAL_SCHEMA_DIR
                        Schema DB directory path when schema-sotre is local
  --type-file TYPE_FILE
                        File path to specified column types
  --tz TZ               Timezone string will be used as default offset in parsing source string if it has no offset
  --api-port API_PORT   Port number of grebe Web API. It is disabled if this is not provided.
  --log-level {DEBUG,INFO,WARN,ERROR}
                        Log level
  --log-format LOG_FORMAT
                        Log format by 'logging' package
  --log-file LOG_FILE   Log file path
  --log-file-count LOG_FILE_COUNT
                        Log file keep count
  --log-file-size LOG_FILE_SIZE
                        Size of each log file
  --retry-max-count RETRY_MAX_COUNT
                        Max count of retry to processing. Message is discard when exceeded max count.
```

This feature is provided by [Lake Weed](https://github.com/tac0x2a/lake_weed).


## Meta data of source

General
```yml
<source_id>:
  types:
    <field_name> : <type>
    <field_name> : <type>

<source_id>:
  types:
    <field_name> : <type>
    <field_name> : <type>
  ...
```

### Specified types

Example
```yml
weather:
  types:
    city: string
    city_code: int
    temperature : double
    location__longitude : double
    location__latitude  : double
  ...
```

This feature is provided by [Lake Weed](https://github.com/tac0x2a/lake_weed).

## Web API
Gerebe provide web api when `--api-port` argument is specified as api port number.

When you launch with `--api-port 8888`, you can access  `http://localhost:8888/` and will be shown `Grebe is running.` message.

### `/` : `GET`
Grebe just show `Grebe is running.` message.

Example: `(200)`
```
Grebe is running.
```


### `/args` : `GET`
Command line argument as json format will be shown.

Example: `(200)`
```json
{"api_port":8888,"dh":"localhost","dp":9000,"local_schema_dir":"schemas","log_file":null,"log_file_count":1000,"log_file_size":1000000,"log_format":"[%(levelname)s] %(asctime)s | %(pathname)s(L%(lineno)s) | %(message)s","log_level":"INFO","mh":"localhost","mp":5672,"queue_name":"nayco","retry_max_count":3,"schema_store":"rdb","type_file":null,"tz":"Asia/Tokyo"}
```

### `/schema_cache` : `GET`
Current schema cache on Grebe.
Schema chash is correspondance Source AMQP topic name and schema of message to destination table name.

Example: There are 2 schema_caches. `(200)`
```json
[
  {
    "schema": {
      "status": "String"
    },
    "source": "rabbitmq_stat_aliveness-test",
    "table": "rabbitmq_stat_aliveness-test_001"
  },
  {
    "schema": {
      "arguments": "String",
      "auto_delete": "UInt8",
      "durable": "UInt8",
      "internal": "UInt8",
      "name": "String",
      "type": "String",
      "user_who_performed_action": "String",
      "vhost": "String"
    },
    "source": "rabbitmq_stat_exchanges",
    "table": "rabbitmq_stat_exchanges_001"
  }
]
```

### `/schema_cache/reload` : `GET`
Reload schema cache from schema source.

Example: Success to reload all schemas. `(200)`
```json
{"result":"Success","schema_count":66,"store":"<class 'grebe.schema_store_clickhouse.SchemaStoreClickhouse'>"}
```


Example: Failed reload schemas. `(500)`
```json
{"result":"Failed","stack_trace":"...traceback.format_exc()..."}
```



## Deploy docker image
```
git tag vX.Y.Z
git push origin master --tags
```

## Contributing

1. Fork it ( https://github.com/tac0x2a/grebe/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
