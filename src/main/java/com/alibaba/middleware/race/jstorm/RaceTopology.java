package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.Consumer;
import com.alibaba.rocketmq.client.exception.MQClientException;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.
 * RaceTopology； 所以这个主类路径一定要正确
 */
public class RaceTopology {

	public static String topologyName = RaceConfig.JstormTopologyName;
	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

	public static void main(String[] args) throws Exception {

		LOG.debug("shittttttttttt!");
		Config conf = new Config();
		try {
			// StormSubmitter.submitTopology(topologyName, conf,
			// builder.createTopology());
			// SetLocalTopology(conf);
			SetRemoteTopology(conf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void SetBuilder(TopologyBuilder builder, Config conf) {
		// ArrayDeque<byte[]> payQueue = new ArrayDeque<byte[]>();
		// ArrayDeque<OrderMessage> orderQueue = new ArrayDeque<OrderMessage>();
		// Consumer consumer = new Consumer(orderQueue, payQueue);
		TairOperatorImpl tair = new TairOperatorImpl(
				RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);

		// try {
		// consumer.startConsume();
		//
		// Thread.sleep(1000);
		// } catch (MQClientException e) {
		// e.printStackTrace();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }

		// int spout_Parallelism_hint = 1;
		// int split_Parallelism_hint = 1;
		// int count_Parallelism_hint = 1;

		// builder.setSpout("spout", new RaceSentenceSpout(),
		// spout_Parallelism_hint);
		// builder.setBolt("split", new SplitSentence(), split_Parallelism_hint)
		// .shuffleGrouping("spout");
		// builder.setBolt("count", new WordCount(), count_Parallelism_hint)
		// .fieldsGrouping("split", new Fields("word"));

		builder.setSpout("paySpout", new PaySpout(), 4);
		// builder.setSpout("platformCacheSpout", new PlatformCacheSpout(
		// orderQueue), 1);

		builder.setBolt("PaySerializeBolt", new PaySerializeBolt(), 4)
				.shuffleGrouping("paySpout");
		builder.setBolt("OrderSerializeBolt", new OrderSerializeBolt(), 6)
				.shuffleGrouping("paySpout");

		builder.setBolt("RatioBolt", new RatioBolt(), 4).shuffleGrouping(
				"PaySerializeBolt");

		ConcurrentHashMap<Long, Double> taobaoMap = new ConcurrentHashMap<Long, Double>();
		builder.setBolt("taobaoBolt", new TaobaoBolt(taobaoMap), 4)
				.shuffleGrouping("PaySerializeBolt");
		builder.setBolt("taobaoWriteBolt", new TaobaoWriteBolt(taobaoMap), 1)
				.shuffleGrouping("PaySerializeBolt");

		ConcurrentHashMap<Long, Double> tianmaoMap = new ConcurrentHashMap<Long, Double>();
		builder.setBolt("tianmaoBolt", new TianmaoBolt(tianmaoMap), 4)
				.shuffleGrouping("PaySerializeBolt");
		builder.setBolt("TianmaoWriteBolt", new TianmaoWriteBolt(tianmaoMap), 1)
				.shuffleGrouping("PaySerializeBolt");
		// builder.setBolt("unknownBolt", new UnknownBolt(),
		// split_Parallelism_hint).shuffleGrouping("PaySerializeBolt");

	}

	public static void SetLocalTopology(Config conf) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		conf.put("bolt.parallel", 1);
		SetBuilder(builder, conf);

		LOG.debug("test");
		LOG.info("Submit log");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, builder.createTopology());

		Thread.sleep(39000);
		cluster.killTopology(topologyName);
		cluster.shutdown();
	}

	public static void SetRemoteTopology(Config conf)
			throws AlreadyAliveException, InvalidTopologyException,
			TopologyAssignException {
		String streamName = (String) conf.get(Config.TOPOLOGY_NAME);
		if (streamName == null) {
			streamName = "SequenceTest";
		}

		TopologyBuilder builder = new TopologyBuilder();
		SetBuilder(builder, conf);
		conf.put(Config.STORM_CLUSTER_MODE, "distributed");

		StormSubmitter.submitTopology(topologyName, conf,
				builder.createTopology());
	}
}