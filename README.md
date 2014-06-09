# StormTopology-AuditActiveLogins

## Description

AuditActiveLogins is a test storm topology for learning purpose.

It counts the User Logins and User Logouts from an audit.log lines, readed from Kafka and insert/update the information in a HBase table.

## Storm topology description

The topology has the following Components:  
  KafkaSpout -> AuditParserBolt -> AuditLoginsCounterBolt -> HBaseBolt
  
### KafkaSpout: 
Connects to a Kafka topic and get the audit lines (previously inserted with flume)  
This spout is based on https://github.com/wurstmeister/storm-kafka-0.8-plus

### AuditParserBolt
Parse the audit line to extract all the information, and insert it in a HashMap structure

### AuditLoginsCounterBolt
Treat the audit line information and pass it to HbaseBolt for insert/update a row.  
In case of user login line, insert a new row with node|user rowkey and counter set to 1 or update an existing row incrementing the counter 1 unit.  
In case of user logout decrement the counter 1 unit (checking previously if the row exist and its counter is greater than zero)

### HBaseBolt
Is the responsible of put/update information in HBaseTable  
This bolt is based on https://github.com/ptgoetz/storm-hbase
  
## Compilation
  TODO: Currently a workaround is neccesary for load hbase configuration properties, hbase-site.xml is included in compilation time, and before compilation is neccesary to change the configuration values. hbase-site.xml is in resources directory
  
```
  mvn clean package
```  
## Config topology
```
# MANDATORY PROPERTIES

# zookeeper hosts and ports (eg: localhost:2181)
zookeeper.hosts=

# kafka topic for read messages
kafka.topic=

# hbase table and column family names to insert results
hbase.table.name=
hbase.column.family=

# OPTIONAL PROPERTIES

# Numbers of workers to parallelize tasks (default 2)
# storm.workers.number=

# Numbers of max task for topology (default 2)
#storm.max.task.parallelism=

# Storm topolgy execution mode (local or cluster, default local)
#storm.execution.mode=

# Storm Topology Name (default AuditActiveLoginsCount)
#storm.topology.name=

# Storm batch emmit interval (default 2000)
#storm.topology.batch.interval.miliseconds

# Time of topology execution, in miliseconds (only in local mode, default 20000)
#storm.local.execution.time=


# CLUSTER PROPERTIES:
# Storm Nimbus host (default localhost)
# storm.nimbus.host=

# Storm Nimbus port (default 6627)
# storm.nimbus.port
```
  
## Run topology

First create the HBaseTable if previously is not created:
```
  hbase shell
  hbase > create 'TableName', 'ColumnFamily'
```

### Storm dependencies

Some libraries are required int storm lib directory:
```
kafka_2.9.2-0.8.0.jar
metrics-core-2.2.0.jar
scala-library-2.9.2.jar
storm-hbase-0.1.0-SNAPSHOT-jar-with-dependencies.jar
storm-kafka-0.8-plus-0.5.0-SNAPSHOT.jar
```
storm-hbase-0.1.0-SNAPSHOT-jar-with-dependencies.jar -> from https://github.com/mvalleavila/storm-kafka-0.8-plus  
storm-kafka-0.8-plus-0.5.0-SNAPSHOT.jar -> from https://github.com/buildoop/storm-hbase
  
### Run\submit topology  
```
storm jar target/AuditActiveLogins-0.1.0.jar org.buildoop.storm.AuditActiveLoginsTopology resources/configuration.properties
```

## See results
```
hbase shell
hbase > scan 'TableName'
```
  

