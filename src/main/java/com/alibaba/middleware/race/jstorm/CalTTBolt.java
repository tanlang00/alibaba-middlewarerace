package com.alibaba.middleware.race.jstorm;

import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CalTTBolt implements IRichBolt, Runnable {
	
	private static final long serialVersionUID = -5928511022530323998L;
	OutputCollector _collector;
	PaymentMessage paymentmessage;
	String createTime;
	double price;
	DecimalFormat format = new DecimalFormat("#.00");
	static Map<String, Double> map_tb = new Hashtable<String, Double>();
	static Map<String, Double> map_tm = new Hashtable<String, Double>();
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple arg0) {
		paymentmessage = (PaymentMessage)arg0.getValue(0);
	//	System.out.println("________________cal______"+ paymentmessage.getMark());
		if(paymentmessage.getMark()== 0){
			createTime = Long.toString((paymentmessage.getCreateTime()/1000/60)*60);
			price = paymentmessage.getPayAmount();
			if(map_tb.containsKey(createTime)){
				map_tb.put(createTime, price+map_tb.get(createTime));
		//		System.out.println("!!!!!!!!!"+ map_tb.get(createTime)+"   :" + createTime);
			}else{
				map_tb.put(createTime, price);
			}
		}else if(paymentmessage.getMark() == 1){
			createTime = Long.toString((paymentmessage.getCreateTime()/1000/60)*60);
			price = paymentmessage.getPayAmount();
			if(map_tm.containsKey(createTime)){
				map_tm.put(createTime, price+map_tm.get(createTime));
			}else{
				map_tm.put(createTime, price);
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
		arg0.declare(new Fields("calculate"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void run() {
		while(true){
		//	System.out.println("======Thread start======");
			try {
				Thread.sleep(RaceConfig.COUNT);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			String[] s_tb = map_tb.keySet().toArray(new String[0]);
			String[] s_tm = map_tm.keySet().toArray(new String[0]);
		//	System.out.println("print++++++:"+ s_tb.length);
		//	System.out.println("printtm++++++"+ s_tm.length);
			for(String s: s_tb){
				double value = map_tb.get(s);
				double value1 = Double.parseDouble(format.format(value));
				String key = "platformTaobao_42853kwnfz_" + s;
				TairOperatorImpl.write(key, value1);
		//		System.out.println("----write_taobao------"+ key + "     : " + value1);
			}
			for(String s: s_tm){
				double value = map_tm.get(s);
				double value1 = Double.parseDouble(format.format(value));
				String key = "platformTmall_42853kwnfz_" + s;
				TairOperatorImpl.write(key, value1);
		//		System.out.println("----write_tianmao------"+ key + "     : " + value1);
			}

		}
		
	}

}
