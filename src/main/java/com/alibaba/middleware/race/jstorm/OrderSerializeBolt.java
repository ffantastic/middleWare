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
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;

public class OrderSerializeBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory
			.getLogger(OrderSerializeBolt.class);
	private OutputCollector collector;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple t) {

		Object o = t.getValue(0);
		if (o instanceof String) {
			String type = (String) o;
			long orderId = -1;
			String orderType = null;
			if ("taobao".equals(type)) {
				Object message = t.getValue(1);
				orderId = getOrderId(message);
				orderType = "taobao";
			} else if ("tMall".equals(type)) {
				Object message = t.getValue(1);
				orderId = getOrderId(message);
				orderType = "tMall";
			}
			// cache order type
			if (orderId != -1) {
				TairOperatorImpl tair = TairOperatorImpl.getInstance();
				tair.write(orderId, orderType);
			}

		}
		collector.ack(t);

	}

	private long getOrderId(Object message) {
		long orderId = -1;
		if (message instanceof byte[]) {
			System.err.println("OrderSerializeBolt");
			byte[] body = (byte[]) message;
			PaymentMessage paymentMessage = RaceUtils.readKryoObject(
					PaymentMessage.class, body);
			orderId = paymentMessage.getOrderId();
			// collector.emit(new Values(paymentMessage));
		}
		return orderId;
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orderSerializeBolt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
