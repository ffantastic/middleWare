package com.alibaba.middleware.race;

import java.util.HashMap;
import java.util.Map;

public class OrderCache {
	public static Integer TMall = 0;
	public static Integer Taobao = 1;
	public static Map<Long, Integer> order2plat = new HashMap<Long, Integer>();
}
