package com.alibaba.middleware.race.jstorm;

import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.Fengzhuang;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PreparedBolt implements IRichBolt, Runnable {
	
	private static final long serialVersionUID = -8602323088021529507L;
	OutputCollector _collector;
	OrderMessage order;
	PaymentMessage paymentmessage;
	long orderId;
	
	Vector<PaymentMessage> vector = new Vector<PaymentMessage>(); 
	static Hashtable<Long, Fengzhuang> map = new Hashtable<Long, Fengzhuang>();
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple arg0) {
		if(arg0.getSourceStreamId().equals(RaceConfig.TAOBAO_ID)){
			order = (OrderMessage)arg0.getValue(0);
			orderId = order.getOrderId();
			Fengzhuang f = new Fengzhuang();
			f.setPlatform(0);
			f.setPrice(order.getTotalPrice());
			map.put(orderId, f);//taobao
			System.out.println("pre******** taobao: "+ order);
		}else if(arg0.getSourceStreamId().equals(RaceConfig.TIANMAO_ID)){
			order = (OrderMessage)arg0.getValue(0);
			orderId = order.getOrderId();
			Fengzhuang f = new Fengzhuang();
			f.setPlatform(1);
			f.setPrice(order.getTotalPrice());
			map.put(orderId, f);//tianmao
		//	System.out.println("pre******** tianmao: "+ order);
		}else if(arg0.getSourceStreamId().equals(RaceConfig.RATIO_ID)){
			paymentmessage = (PaymentMessage)arg0.getValue(0);
		//	System.out.println("pre******** pay: "+ paymentmessage);
			if(map.containsKey(paymentmessage.getOrderId())){
				paymentmessage.setMark(map.get(paymentmessage.getOrderId()).getPlatform());
				if(map.get(paymentmessage.getOrderId()).subprice(paymentmessage.getPayAmount())){
					map.remove(paymentmessage.getOrderId());
				//	System.out.println("remove-------------------------------------------");
				}
				_collector.emit(new Values(paymentmessage));
			//	System.out.println("***************pre***"+ paymentmessage);
			}else{
				vector.add(paymentmessage);
			}
		}

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		_collector = arg2;
		new Thread(this).start();
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("prepared"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(RaceConfig.COUNT_P);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Vector<PaymentMessage> v = (Vector<PaymentMessage>)vector.clone();
			for(PaymentMessage pay:v){
				if(map.containsKey(pay.getOrderId())){
					paymentmessage.setMark(map.get(paymentmessage.getOrderId()).getPlatform());
					if(map.get(paymentmessage.getOrderId()).subprice(paymentmessage.getPayAmount())){
						map.remove(paymentmessage.getOrderId());
					}
					_collector.emit(new Values(pay));
				//	System.out.println("***************preThread***"+ pay);
					vector.remove(pay);
				}
			}
			
		}
	}

}
