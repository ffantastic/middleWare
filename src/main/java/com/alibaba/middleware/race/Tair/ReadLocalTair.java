package com.alibaba.middleware.race.Tair;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import com.alibaba.middleware.race.RaceConfig;

public class ReadLocalTair {

	public static void main(String[] args) throws FileNotFoundException {
		TairOperatorImpl tair = new TairOperatorImpl(
				RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);
		int from = 1468073940;
		int to = 1468074180;
		PrintWriter pw = new PrintWriter(
				"/home/tianchi/test_result/readtair.txt");
		// for (int i = from; i < to; i++) {
		// System.err.println(i);
		// Object vTaobao = tair.get(RaceConfig.prex_taobao + i);
		// Object vTmall = tair.get(RaceConfig.prex_tmall + i);
		// Object vRatio = tair.get(RaceConfig.prex_ratio + i);
		// if (vTaobao != null) {
		// pw.println(RaceConfig.prex_taobao + i + " : " + vTaobao);
		// // System.out
		// // .println(RaceConfig.prex_taobao + i + " : " + vTaobao);
		// }
		// if (vTmall != null) {
		// pw.println(RaceConfig.prex_tmall + i + " : " + vTmall);
		// // System.out.println(RaceConfig.prex_tmall + i + " : " +
		// // vTmall);
		// }
		// if (vRatio != null) {
		// pw.println(RaceConfig.prex_ratio + i + " : " + vRatio);
		// // System.out.println(RaceConfig.prex_ratio + i + " : " +
		// // vRatio);
		// }
		// }
		readWord(pw, tair);
		pw.flush();
		pw.close();
	}

	private static void readWord(PrintWriter pw, TairOperatorImpl tair) {
		String[] keys = { "to", "ten", "peppers", "test", "one", "broadcast",
				"nine", "emergency", "little", "where", "every", "eight",
				"went", "was", "six", "of", "snow", "three", "peck", "only",
				"fleese", "four", "had", "marry", "sure", "pickeled", "whos",
				"is", "five", "peter", "a", "white", "as", "lamb", "the",
				"piper", "and", "that", "two", "system", "picked", "seven",
				"this", "go" };
		for (String key : keys) {
			Object v = tair.get(key);
			if (v != null)
				pw.println(key + " :" + v);

		}

	}

}
