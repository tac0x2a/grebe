# Grebe
[![Build Status](https://travis-ci.org/tac0x2a/grebe.svg?branch=master)](https://travis-ci.org/tac0x2a/grebe)

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

Grebe is forwarder JSON message from RabbitMQ to Clickhouse.

1. Receive JSON message from RabbitMQ specified queue on RabbitMQ.
2. Parse, convert, and create table if need it. Each attributes are mapped to columns on Clickhouse table.

## Usage

```sh
$ pip install -r requirements.txt
$ chmod +x grebe.py

$ ./grebe.py biwako -mh 192.168.11.200 -dh 192.168.11.200 -dp 19000
```

## Test
```sh
$ pytest
```

## Help

```sh
$ ./grebe.py -h

usage: grebe.py [-h] [-mh MH] [-mp MP] [-dh DH] [-dp DP]
                [--log-level {DEBUG,INFO,WARN,ERROR}]
                [--log-format LOG_FORMAT] [--log-file LOG_FILE]
                [--log-file-count LOG_FILE_COUNT]
                [--log-file-size LOG_FILE_SIZE]
                queue_name

Forward JSON message from RabbitMQ to Clickhouse

positional arguments:
  queue_name            Queue name to subscribe on RabbitMQ

optional arguments:
  -h, --help            show this help message and exit
  -mh MH                RabbitMQ host
  -mp MP                RabbitMQ port
  -dh DH                Clickhouse host
  -dp DP                Clickhouse port by native connection
  --log-level {DEBUG,INFO,WARN,ERROR}
                        Log level
  --log-format LOG_FORMAT
                        Log format by 'logging' package
  --log-file LOG_FILE   Log file directory
  --log-file-count LOG_FILE_COUNT
                        Log file keep count
  --log-file-size LOG_FILE_SIZE
                        Size of each log file
```

## Contributing

1. Fork it ( https://github.com/tac0x2a/grebe/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request