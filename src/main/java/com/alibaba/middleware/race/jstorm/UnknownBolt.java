package com.alibaba.middleware.race.jstorm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.OrderCache;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class UnknownBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(UnknownBolt.class);
	private OutputCollector collector;
	public long count = 0;

	@Override
	public void cleanup() {
//		String path = "E:/tianchi/test_result/unknown.txt";
//		PrintWriter pw = null;
//		try {
//			pw = new PrintWriter(path);
//			pw.print(count);
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			if (pw != null)
//				pw.close();
//		}

	}

	@Override
	public void execute(Tuple t) {
		Object o = t.getValue(0);
		if (o instanceof PaymentMessage) {
			// LOG.debug("taobao bolt " + o);
			PaymentMessage pay = (PaymentMessage) o;

			Integer platform = OrderCache.order2plat.get(pay.getOrderId());
			if (platform == null) {
				count++;
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
		declarer.declare(new Fields("unknown_bolt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
