package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.tair.json.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RockMqSpout implements IRichSpout {
	
	private static final long serialVersionUID = 4684841464507100055L;
	
	SpoutOutputCollector _collector;
	PaymentMessage paymentMessage;
	OrderMessage orderMessage;
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
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub

	}

	@Override
	public void open(Map paramMap, TopologyContext paramTopologyContext,
			SpoutOutputCollector paramSpoutOutputCollector) {
			_collector = paramSpoutOutputCollector;
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.setInstanceName(Double.toString(Math.random()*1000));
			//consumer.setNamesrvAddr(RaceConfig.RocketMqServer);
			try {
				consumer.subscribe(RaceConfig.MqPayTopic, "*");
				consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
				consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			} catch (MQClientException e1) {
				e1.printStackTrace();
			}
			
			consumer.registerMessageListener(new MessageListenerConcurrently() {

	            @Override
	            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
	            		for (MessageExt msg : msgs) {
	            			byte [] body = msg.getBody();
	            			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
	            				//Info: 生产者停止生成数据, 并不意味着马上结束
	            				System.out.println("Got the end signal");
	            				continue;
	            			}
	            			if(msg.getTopic().equals(RaceConfig.MqPayTopic)){
	            				paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
//	            				JSONObject root = new JSONObject();
//	            				root.put("key", RaceConfig.MqPayTopic);
//	            				root.put(RaceConfig.MqPayTopic, paymentMessage);
		            			_collector.emit(RaceConfig.RATIO_ID,new Values(paymentMessage));
	            			}else if(msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)){
	            				orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//	            				JSONObject root = new JSONObject();
//	            				root.put("key", RaceConfig.MqTaobaoTradeTopic);
//	            				root.put(RaceConfig.MqTaobaoTradeTopic, orderMessage);
	            		//		System.out.println("****************"+orderMessage);
		            			_collector.emit(RaceConfig.TAOBAO_ID,new Values(orderMessage));
	            			}else if(msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
	            				orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
//	            				JSONObject root = new JSONObject();
//	            				root.put("key", RaceConfig.MqTmallTradeTopic);
//	            				root.put(RaceConfig.MqTmallTradeTopic, orderMessage);
		            			_collector.emit(RaceConfig.TIANMAO_ID,new Values(orderMessage));
	            			}
	    
//	            			System.out.println(paymentMessage);
	            		}
	            		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	            }
			});
				
			try {
				System.out.println("consumer is started!!!");
				consumer.start();
			} catch (MQClientException e) {
				e.printStackTrace();
			}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("rocketmq"));
		arg0.declareStream(RaceConfig.TAOBAO_ID, new Fields("Taobao"));
		arg0.declareStream(RaceConfig.TIANMAO_ID, new Fields("Tianmao"));
		arg0.declareStream(RaceConfig.RATIO_ID, new Fields("ratio"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
