package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

	// 这些是写tair key的前缀
	public static String prex_tmall = "platformTmall_41087l2ki7_";
	public static String prex_taobao = "platformTaobao_41087l2ki7_";
	public static String prex_ratio = "ratio_41087l2ki7_";

	// 这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
	public static String JstormTopologyName = "41087l2ki7";
	public static String MetaConsumerGroup = "41087l2ki7";
	public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
	public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
	public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

//	public static String TairConfigServer = "192.168.140.129:5198";
//	public static String TairSalveConfigServer = "192.168.140.129:5198";
//	public static String TairGroup = "group_1";
//	public static Integer TairNamespace = 0;

	 public static String TairConfigServer = "10.101.72.127:5198";
	 public static String TairSalveConfigServer = "10.101.72.128:5198";
	 public static String TairGroup = "group_tianchi";
	 public static Integer TairNamespace = 30845;
}
