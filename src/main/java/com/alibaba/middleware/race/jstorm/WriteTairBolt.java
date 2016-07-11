package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WriteTairBolt implements IRichBolt {

	private TairOperatorImpl tair;
	private static Logger LOG = LoggerFactory.getLogger(WriteTairBolt.class);
	private OutputCollector collector;

	public WriteTairBolt(TairOperatorImpl tair) {
		this.tair = tair;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple t) {
		Object o = t.getValue(0);
		if (o instanceof PaymentMessage) {

		} else {
			String key = t.getString(0);
			Serializable value = (Serializable) t.getValue(1);
			tair.write(key, value);
		}
		collector.ack(t);

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("write_tair_bolt"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
