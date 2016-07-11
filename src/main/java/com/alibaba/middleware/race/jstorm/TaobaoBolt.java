package com.alibaba.middleware.race.jstorm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.OrderCache;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TaobaoBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(TaobaoBolt.class);
	private OutputCollector collector;
	// private TairOperatorImpl tair;
	public Map<Long, Double> result = new ConcurrentHashMap<Long, Double>();

	public TaobaoBolt(TairOperatorImpl tair) {
		// this.tair = tair;
	}

	public TaobaoBolt(Map<Long, Double> result) {
		this.result = result;
	}

	@Override
	public void cleanup() {
		// String path = "/home/tianchi/test_result/taobao.txt";
		// PrintWriter pw = null;
		// try {
		// pw = new PrintWriter(path);
		// for (Long key : result.keySet())
		// pw.println(key + " " + result.get(key));
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } finally {
		// if (pw != null)
		// pw.close();
		// }

		// try {
		// TairOperatorImpl tair = TairOperatorImpl.getInstance();
		// for (Long k : result.keySet()) {
		// tair.write(RaceConfig.prex_taobao + k, result.get(k));
		// // result.remove(k);
		// }
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

	}

	@Override
	public void execute(Tuple t) {
		LOG.debug("111");
		Object o = t.getValue(0);
		if (o instanceof PaymentMessage) {
			// LOG.debug("taobao bolt " + o);
			PaymentMessage pay = (PaymentMessage) o;

			// Integer platform = OrderCache.order2plat.get(pay.getOrderId());
			TairOperatorImpl tair = TairOperatorImpl.getInstance();
			Object type = tair.get(pay.getOrderId());
			if (type != null && type instanceof String) {
				if ("taobao".equals(type)) {
					Long min = RaceUtils.getMinute(pay.getCreateTime());
					Double v = result.get(min);
					if (v == null)
						result.put(min, pay.getPayAmount());
					else
						result.put(min, pay.getPayAmount() + v);
				}
			}

		}
		// (t.getString(0));
		collector.ack(t);

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("taobao_bolt"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
