package com.alibaba.middleware.race.jstorm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RatioBolt implements IRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(RatioBolt.class);
	private OutputCollector collector;
	public AtomicLong pc = new AtomicLong(1);
	public AtomicLong wireless = new AtomicLong(0);

	// private TairOperatorImpl tair;
	private long lastTime = -1;
	private long writePeriod_Min = 0;

	public RatioBolt(TairOperatorImpl tair) {
		// this.tair = tair;
	}

	public RatioBolt() {
		// this.tair = new TairOperatorImpl(RaceConfig.TairConfigServer,
		// RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
		// RaceConfig.TairNamespace);
	}

	@Override
	public void cleanup() {
		// String path = "/home/tianchi/test_result/ratio.txt";
		// PrintWriter pw = null;
		// try {
		// pw = new PrintWriter(path);
		// pw.print(pc + " / " + wireless);
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } finally {
		// if (pw != null)
		// pw.close();
		// }

		double r = 1.0 * wireless.get() / pc.get();
		writeTair(r);
	}

	private void writeTair(double r) {

		TairOperatorImpl tair = TairOperatorImpl.getInstance();
		tair.write(RaceConfig.prex_ratio + lastTime, r);

	}

	@Override
	public void execute(Tuple t) {
		Object o = t.getValue(0);
		if (o instanceof PaymentMessage) {
			// LOG.debug("taobao bolt " + o);
			PaymentMessage pay = (PaymentMessage) o;
			if (pay.getPayPlatform() == 0)
				pc.incrementAndGet();
			else
				wireless.incrementAndGet();
			long time = RaceUtils.getMinute(pay.getCreateTime());
			if (lastTime != -1 && time != lastTime) {
				double r = 1.0 * wireless.get() / pc.get();
				writeTair(r);
				// tair.write(RaceConfig.prex_ratio + lastTime, r);
			}
			lastTime = time;
			// collector.emit(new Values(pay));

		}
		collector.ack(t);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Ratio_bolt"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
