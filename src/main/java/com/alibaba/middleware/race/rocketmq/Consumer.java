package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.OrderCache;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class Consumer {

	ArrayDeque<byte[]> taobao_orderQueue;
	ArrayDeque<byte[]> tMall_orderQueue;
	ArrayDeque<byte[]> payQueue;
	private static Consumer holder;

	private Consumer(ArrayDeque<byte[]> taobao_orderQueue,
			ArrayDeque<byte[]> tMall_orderQueue, ArrayDeque<byte[]> payQueue) {
		this.taobao_orderQueue = taobao_orderQueue;
		this.tMall_orderQueue = tMall_orderQueue;
		this.payQueue = payQueue;
	}

	public static synchronized Consumer getInstance(
			ArrayDeque<byte[]> taobao_orderQueue,
			ArrayDeque<byte[]> tMall_orderQueue, ArrayDeque<byte[]> payQueue) {
		if (holder == null) {
			holder = new Consumer(taobao_orderQueue, tMall_orderQueue, payQueue);
			return holder;
		}
		return null;
	}

	public void startConsume() throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				RaceConfig.MetaConsumerGroup);

		/**
		 * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
		 * 如果非第一次启动，那么按照上次消费的位置继续消费
		 */
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		// OrderCache.order2plat = new HashMap<Long, Integer>();
		// consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET);
		// 在本地搭建好broker后,记得指定nameServer的地址
		// consumer.setNamesrvAddr("127.0.0.1:9876");

		consumer.subscribe(RaceConfig.MqPayTopic, "*");
		consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
		consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");

		consumer.registerMessageListener(new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(
					List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				for (MessageExt msg : msgs) {
					byte[] body = msg.getBody();
					if (body.length == 2 && body[0] == 0 && body[1] == 0) {
						// Info: 生产者停止生成数据, 并不意味着马上结束
						// System.out.println("Got the end signal");
						continue;
					}
					if (RaceConfig.MqPayTopic.equals(msg.getTopic())) {
						// PaymentMessage paymentMessage = RaceUtils
						// .readKryoObject(PaymentMessage.class, body);
						payQueue.add(body);
					} else {
						// OrderMessage orderMessage = RaceUtils.readKryoObject(
						// OrderMessage.class, body);
						if (RaceConfig.MqTaobaoTradeTopic.equals(msg.getTopic()))
							// OrderCache.order2plat.put(
							// orderMessage.getOrderId(),
							// OrderCache.Taobao);
							taobao_orderQueue.add(body);
						else {
							// OrderCache.order2plat.put(
							// orderMessage.getOrderId(), OrderCache.TMall);
							tMall_orderQueue.add(body);
						}
						// orderQueue.push(orderMessage);
						// System.out.println(orderMessage);
					}
				}
//				System.out.println("consumer " + payQueue.size() + " "
//						+ taobao_orderQueue.size() + " "
//						+ tMall_orderQueue.size());
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		consumer.start();
		System.out.println("Consumer Started.");
	}

	public static void main(String[] args) throws InterruptedException,
			MQClientException {
		// Consumer myCousmer = new Consumer(new ArrayDeque<OrderMessage>(),
		// new ArrayDeque<byte[]>());
		// myCousmer.startConsume();
	}
}
