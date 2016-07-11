package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class PlatformCacheSpout implements IRichSpout {

	private static Logger LOG = LoggerFactory.getLogger(PlatformCacheSpout.class);
	private SpoutOutputCollector _collector;
	// private int sendNumPerNexttuple;
	private LinkedBlockingDeque<OrderMessage> queue;

	public PlatformCacheSpout(LinkedBlockingDeque<OrderMessage> queue) {
		this.queue = queue;
	}

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object id) {
		// collector.emit(new Values(id), id);

	}

	@Override
	public void nextTuple() {
		// int size=queue.

	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		_collector = arg2;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("platform_cache_spout"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
