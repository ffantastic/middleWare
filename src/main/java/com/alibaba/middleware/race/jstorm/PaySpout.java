package com.alibaba.middleware.race.jstorm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.OrderCache;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.Consumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.google.common.cache.Cache;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class PaySpout implements IRichSpout {
	private static Logger LOG = LoggerFactory.getLogger(PaySpout.class);
	private SpoutOutputCollector _collector;
	// private int sendNumPerNexttuple;
	private ArrayDeque<byte[]> payQueue;
	private ArrayDeque<byte[]> taobaoOrderQueue;
	private ArrayDeque<byte[]> tmallOrderQueue;

	private transient Consumer consumer = null;

	public PaySpout() {
		// this.queue = queue;
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
		_collector.emit(new Values(id), id);
	}

	@Override
	public void nextTuple() {

		Object pay = payQueue.poll();
		if (pay != null) {
			_collector.emit(new Values("pay", pay));
		}
		Object taobao = taobaoOrderQueue.poll();
		if (taobao != null) {
			_collector.emit(new Values("taobao", taobao));
		}
		Object tMall = tmallOrderQueue.poll();
		if (tMall != null) {
			_collector.emit(new Values("tMall", tMall));
		}
		// String path = "/home/tianchi/test_result/tessst.txt";
		// PrintWriter pw = null;
		// try {
		// pw=new PrintWriter(path);
		// pw.println("PaySpout , length ............." + queue.size());
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } finally {
		// if (pw != null)
		// pw.close();
		// }
		// LOG.debug("111");
		// int n = sendNumPerNexttuple;
		//
		// Utils.sleep(100);
		// _collector.emit(new Values("123123123123 123123123123"));

	}

	@Override
	public void open(Map conf, TopologyContext arg1, SpoutOutputCollector arg2) {
		_collector = arg2;
		payQueue = new ArrayDeque<byte[]>();
		taobaoOrderQueue = new ArrayDeque<byte[]>();
		tmallOrderQueue = new ArrayDeque<byte[]>();

		this.consumer = Consumer.getInstance(taobaoOrderQueue, tmallOrderQueue,
				payQueue);
		if (this.consumer != null)
			try {
				this.consumer.startConsume();
			} catch (MQClientException e) {
				// TODO Auto-generated catch blockF
				e.printStackTrace();
			}
		// sendNumPerNexttuple =
		// JStormUtils.parseInt(conf.get("send.num.each.time"), 1);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type","pay_spout"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
