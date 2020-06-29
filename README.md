# franz

Your Swiss-Army Knife tool for interacting with Kafka.

`franz` provides you various helper tools to work with and debug Kafka such as consuming or producing messages and
managing topics or ACLs.

## Installation
```console
go get github.com/open-ch/franz
```

## Usage
To connect to your Kafka cluster and optionally to a schema registry, `franz` reads from a configuration file that can be
placed in one of the following paths
- `~/.config/franz`
- `/opt/franz/etc`
- any path you wish, passed in using `--config /path/to/config`
A configuration file has roughly the form of [config.yml.sample](config.yml.sample).

```console
$ franz help
Usage:
  franz [command]

Available Commands:
  acls        List and Set Kafka ACLs
  consume     Consume a specific kafka topic
  help        Help about any command
  produce     Produce messages in the specified topic.
  registry    Interact with the schema registry
  status      Show the status of Kafka System
  topics      Manage topics
```

You can get more information about any command by running `franz help [command]`.

## Examples
### List relevant information for all Kafka brokers
```console
$ franz status --table
+-------------------------------------------+-----------+--------+---------------------+---------------------+----------+
|                   TOPIC                   | PARTITION | LEADER |      REPLICAS       |         ISR         | INTERNAL |
+-------------------------------------------+-----------+--------+---------------------+---------------------+----------+
| notifications.users                       | 0         | 10364  | 10364, 10363        | 10363, 10364        | false    |
| notifications.users                       | 2         | 10363  | 10363, 10365        | 10363, 10365        | false    |
| notifications.users                       | 1         | 10365  | 10365, 10364        | 10364, 10365        | false    |
| analytics.avro.pageviews                  | 0         | 10365  | 10365, 10363        | 10363, 10365        | false    |
...
```

### Continuously read from a topic
```console
$ franz consume notifications.users --follow
{
  "Topic": "notifications.users",
  "Timestamp": "2020-06-24T09:43:32.443Z",
  "Partition": 3,
  "Key": "...",
  "Value": "...",
  "Offset": 5755325
}
{
  "Topic": "notifications.users",
  "Timestamp": "2020-06-24T09:43:32.443Z",
  "Partition": 2,
  "Key": "...",
  "Value": "...",
  "Offset": 5755326
}
...
```

## Contributors
Due to a migration of the codebase, some authors might not show up in the git history even though they contributed to
this project:
- [Symeon Meichanetzoglou](https://github.com/symaras)
- [Daniel Aschwanden](https://github.com/nimdanitro)
- Ines Haymann
- [Huu-An Cao](https://github.com/Huuancao)
- [Anastasios Zouzias](https://github.com/zouzias)
- [Florian Lehner](https://github.com/florianl)

## License
Please see [LICENSE](LICENSE).
