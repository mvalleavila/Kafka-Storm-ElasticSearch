package org.buildoop.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.buildoop.storm.bolts.AuditParserBolt;
import org.buildoop.storm.bolts.AuditLoginsCounterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.hmsonline.storm.contrib.bolt.elasticsearch.ElasticSearchBolt;
import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.DefaultTupleMapper;
import com.hmsonline.storm.contrib.bolt.elasticsearch.mapper.TupleMapper;
import com.hmsonline.storm.elasticsearch.StormElasticSearchConstants;

public class AuditActiveLoginsTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(AuditActiveLoginsTopology.class);

	private final BrokerHosts kafkaBrokerHosts;

	public AuditActiveLoginsTopology(String zookeeperHosts) {
		kafkaBrokerHosts = new ZkHosts(zookeeperHosts);
	}

	public StormTopology buildTopology(Properties properties) {
		
		// Load properties for the storm topology
		String kafkaTopic = properties.getProperty("kafka.topic");
		
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "",	"storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();

		// Specific audit logs analysis bolts
		AuditLoginsCounterBolt loginCounterbolt = new AuditLoginsCounterBolt();
		AuditParserBolt auditParserBolt = new AuditParserBolt();
		
		// Elastic search bolt
		TupleMapper tupleMapper = new DefaultTupleMapper();
		ElasticSearchBolt elasticSearchBolt = new ElasticSearchBolt(tupleMapper);

		// Topology scheme: KafkaSpout -> auditParserBolt -> loginCounterBolt -> elasticSearchBolt
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
		builder.setBolt("ParseBolt", auditParserBolt, 1).shuffleGrouping("KafkaSpout");
		builder.setBolt("CountBolt", loginCounterbolt, 1).shuffleGrouping("ParseBolt");
		builder.setBolt("ElasticSearchBolt", elasticSearchBolt, 1)
		.fieldsGrouping("CountBolt", new Fields("id", "index", "type", "document"));

		return builder.createTopology();
	}

	private static List<String> parseZkHosts(String zkNodes) {

		String[] hostsAndPorts = zkNodes.split(",");
		List<String> hosts = new ArrayList<String>(hostsAndPorts.length);

		for (int i = 0; i < hostsAndPorts.length; i++) {
			hosts.add(i, hostsAndPorts[i].split(":")[0]);
		}
		return hosts;
	}

	private static int parseZkPort(String zkNodes) {

		String[] hostsAndPorts = zkNodes.split(",");
		int port = Integer.parseInt(hostsAndPorts[0].split(":")[1]);
		return port;
	}
	

	private static Properties loadProperties(String propertiesFile) throws Exception {
		Properties properties = new Properties();
		FileInputStream in = new FileInputStream(propertiesFile);
		properties.load(in);
		in.close();
		
		return properties;
	}
	
	private static void loadTopologyPropertiesAndSubmit(Properties properties,
			Config config) throws Exception {
		
		String stormExecutionMode = properties.getProperty("storm.execution.mode","local");
		int stormWorkersNumber = Integer.parseInt(properties.getProperty("storm.workers.number","2"));
		int maxTaskParallism = Integer.parseInt(properties.getProperty("storm.max.task.parallelism","2"));
		String topologyName = properties.getProperty("storm.topology.name","topologyName");
		String zookeeperHosts = properties.getProperty("zookeeper.hosts");
		int topologyBatchEmitMillis = Integer.parseInt(
				properties.getProperty("storm.topology.batch.interval.miliseconds","2000"));
		
		// How often a batch can be emitted in a Trident topology.
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, topologyBatchEmitMillis);
		config.setNumWorkers(stormWorkersNumber);
		config.setMaxTaskParallelism(maxTaskParallism);
		
		AuditActiveLoginsTopology auditActiveLoginsTopology = new AuditActiveLoginsTopology(zookeeperHosts);
		StormTopology stormTopology = auditActiveLoginsTopology.buildTopology(properties);
		
		// Elastic Search specific properties
		config.put(StormElasticSearchConstants.ES_HOST, properties.getProperty("elasticsearch.host", "localhost"));
		config.put(StormElasticSearchConstants.ES_PORT, (Integer.parseInt(properties.getProperty("elasticsearch.port", "9300"))));
		config.put(StormElasticSearchConstants.ES_CLUSTER_NAME, properties.getProperty("elasticsearch.cluster.name"));
		config.put("elasticsearch.index", properties.getProperty("elasticsearch.index"));
		config.put("elasticsearch.type", properties.getProperty("elasticsearch.type"));
	
		switch (stormExecutionMode){
			case ("cluster"):
				String nimbusHost = properties.getProperty("storm.nimbus.host","localhost");
				String nimbusPort = properties.getProperty("storm.nimbus.port","6627");
				config.put(Config.NIMBUS_HOST, nimbusHost);
				config.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(nimbusPort));
				config.put(Config.STORM_ZOOKEEPER_PORT, parseZkPort(zookeeperHosts));
				config.put(Config.STORM_ZOOKEEPER_SERVERS, parseZkHosts(zookeeperHosts));
				StormSubmitter.submitTopology(topologyName, config, stormTopology);
				break;
			case ("local"):
			default:
				int localTimeExecution = Integer.parseInt(properties.getProperty("storm.local.execution.time","20000"));
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, config, stormTopology);
				Thread.sleep(localTimeExecution);
				cluster.killTopology(topologyName);
				cluster.shutdown();
				System.exit(0);
		}		
	}

	public static void main(String[] args) throws Exception {
		
		String propertiesFile = args[0];
		Properties properties = loadProperties(propertiesFile);
		Config config = new Config();
	
		loadTopologyPropertiesAndSubmit(properties,config);

	}

}
