package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.OrderCache;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

public class PaySerializeBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(PaySerializeBolt.class);
	private OutputCollector collector;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple t) {

		Object o = t.getValue(0);
		if (o instanceof String) {
			String type = (String) o;
			if ("pay".equals(type)) {
				Object message = t.getValue(1);
				if (message instanceof byte[]) {
					System.err.println("PaySerializeBolt");
					// LOG.debug("taobao bolt " + o);
					byte[] body = (byte[]) message;
					PaymentMessage paymentMessage = RaceUtils.readKryoObject(
							PaymentMessage.class, body);
					collector.emit(new Values(paymentMessage));
				}
			}
		}
		collector.ack(t);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("paySerializeBolt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
