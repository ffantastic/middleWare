package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;

public class WordCount implements IRichBolt {
	OutputCollector collector;
	Map<String, Integer> counts = new HashMap<String, Integer>();

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		System.err.println(word);
		Integer count = counts.get(word);
		if (count == null)
			count = 0;
		counts.put(word, ++count);
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void cleanup() {
		System.err.println("cleaning up.............................");
		String path = "/home/tianchi/test_result/wordcount.txt";
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(path);
			pw.println("cleaning up.............................");
			TairOperatorImpl tair = TairOperatorImpl.getInstance();
			for (String key : counts.keySet()) {
				pw.println(key + " " + counts.get(key));
				tair.write(key, counts.get(key));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			pw.println(e.toString());
			for (StackTraceElement ste : e.getStackTrace()) {
				pw.println(ste.toString());
			}

		} finally {
			if (pw != null)
				pw.close();
		}

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}