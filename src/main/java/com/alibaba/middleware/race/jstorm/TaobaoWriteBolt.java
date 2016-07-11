package com.alibaba.middleware.race.jstorm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

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

public class TaobaoWriteBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(TaobaoWriteBolt.class);
	private OutputCollector collector;
	private long lastTime = -1;
	private long writePeriod_Min = 0;
	// private TairOperatorImpl tair;
	public Map<Long, Double> result = new HashMap<Long, Double>();

	public TaobaoWriteBolt(TairOperatorImpl tair) {
	}

	public TaobaoWriteBolt(Map<Long, Double> result) {
		this.result = result;
	}

	@Override
	public void cleanup() {

		TairOperatorImpl tair = TairOperatorImpl.getInstance();
		for (Long k : result.keySet()) {
			tair.write(RaceConfig.prex_taobao + k, result.get(k));
			// result.remove(k);
		}

	}

	@Override
	public void execute(Tuple t) {
		Object o = t.getValue(0);
		if (o instanceof PaymentMessage) {
			// LOG.debug("taobao bolt " + o);
			PaymentMessage pay = (PaymentMessage) o;

			long time = RaceUtils.getMinute(pay.getCreateTime());
			if (lastTime != -1 && time - lastTime > writePeriod_Min) {
				TairOperatorImpl tair = TairOperatorImpl.getInstance();
				Object type = tair.get(pay.getOrderId());
				if (type != null && type instanceof String) {
					if ("taobao".equals(type)) {
						for (Long k : result.keySet()) {
							if (k <= lastTime) {
								boolean s = tair.write(RaceConfig.prex_taobao
										+ k, result.get(k));
								result.remove(k);
							}
						}
						lastTime = time;
					}
				}
			} else if (lastTime == -1) {
				lastTime = time;
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
		declarer.declare(new Fields("taobao_write_bolt"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
