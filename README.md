# kadmin Kafka CLI

This application is still work in progress - the interface might change a lot and needs to be built
locally to be used. To build it run:

```shell
./gradlew installDist
```

The executable can then be found under `./build/install/kadmin/bin/kadmin`. The get an overview over
the subcommands and usage us the global help:

```shell
kadmin --help
```

or to get help for a specific sub-command use:

```shell
kadmin <some subcommnand> --help
```

# Global settings

The broker to connect to can be set via an environment variable like this:

```shell
export KAFKA_BOOTSTRAP_SERVERS=mykafka:9092
```

# Subcommands

## List Partitions

The `list` command allows to list partitions based on filter criteria. Currently, filtering can be
done by topic prefix or by number of replicas.

For example to get all partitions of topics with names starting with `tenant_x_` run:

```shell
kadmin list -t tenant_x_
```

To get all partitions with a replication factor of 2 use:

```shell
kadmin list -r 2
```

## Ensure Replicas

The `ensure-replicas` command can be used to increase the numbers of replicas of certain partitions
to a given number. The command assumes that every broker has the same storage capacity and tries to
even out storage size of every broker.

For defining which partitions should be affected the same filters can be used as in the `list`
command.

By default, the command applies no changes and just prints out the repartitioning that would happen.
For example, to increase all topics with names starting with `my_customer_` to at least 2 replicas
use:

```shell
kadmin ensure-replicas 2 -t my_customer_
```

The above command will just list what would be done. To actually apply it run:

```shell
kadmin ensure-replicas 2 -t my_customer_ --apply
```











