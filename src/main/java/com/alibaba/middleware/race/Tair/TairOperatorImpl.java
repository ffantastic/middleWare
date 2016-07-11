package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/ group
 * 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl implements Serializable {

	private final int namespace;
	private final DefaultTairManager tairManager;

	private static TairOperatorImpl holder = null;

	public TairOperatorImpl(String masterConfigServer,
			String slaveConfigServer, String groupName, int namespace) {
		this.namespace = namespace;
		// 创建config server列表
		List<String> confServers = new ArrayList<String>();
		confServers.add(masterConfigServer);
		confServers.add(slaveConfigServer); // 可选

		// 创建客户端实例
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName(groupName);
		// 初始化客户端
		tairManager.init();
		holder = this;
	}

	public static TairOperatorImpl getInstance() {
		if (holder != null)
			return holder;
		return new TairOperatorImpl(RaceConfig.TairConfigServer,
				RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
				RaceConfig.TairNamespace);
		// throw new Exception("null TairOopertorImpl!!!");
	}

	public boolean write(Serializable key, Serializable value) {
		ResultCode rc = tairManager.put(namespace, key, value);
		if (rc.isSuccess()) {
			return true;
		} else if (ResultCode.VERERROR.equals(rc)) {
			// 版本错误的处理代码
		} else {
			// 其他失败的处理代码
		}
		return false;
	}

	public Object get(Serializable key) {
		Result<DataEntry> result = tairManager.get(namespace, key);
		if (result.isSuccess()) {
			DataEntry entry = result.getValue();
			if (entry != null) {
				// 数据存在
				return entry.getValue();
			} else {
				return null;
			}
		} else {
			// 异常处理
		}
		return null;
	}

	public boolean remove(Serializable key) {
		ResultCode rc = tairManager.delete(namespace, key);
		if (rc.isSuccess()) {
			return true;
			// 删除成功
		} else {
			// 删除失败
		}
		return false;
	}

	public void close() {
	}

	// 天猫的分钟交易额写入tair
	public static void main(String[] args) throws Exception {
		TairOperatorImpl tairOperator = new TairOperatorImpl(
				RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
				RaceConfig.TairGroup, RaceConfig.TairNamespace);

		// 假设这是付款时间
		Long millisTime = System.currentTimeMillis();
		// 由于整分时间戳是10位数，所以需要转换成整分时间戳
		Long minuteTime = (millisTime / 1000 / 60) * 60;
		// 假设这一分钟的交易额是100;
		Double money = 100.0;
		// 写入tair
		tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
		System.out.println(RaceConfig.prex_tmall + minuteTime + " "
				+ tairOperator.get(RaceConfig.prex_tmall + minuteTime));

		// TairOperatorImpl tairOperator2 = new TairOperatorImpl(
		// RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
		// RaceConfig.TairGroup, RaceConfig.TairNamespace);
	}
}
