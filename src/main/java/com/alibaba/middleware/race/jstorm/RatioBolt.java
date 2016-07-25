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

public class RatioBolt implements IRichBolt,Runnable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7187816634028899664L;
	OutputCollector collector;
	PaymentMessage payment;
	long createTime;
	double price;
	short platform;
	String platform_time;
	String platform_time_other;
	double result = 0;
	double result1;
	double startTime;
	double endTime;
	DecimalFormat format = new DecimalFormat("#.00");
	static Map<String, Double> map = new Hashtable<String, Double>();
	static Map<String, Double> map1 = new Hashtable<String, Double>();
	static int count = RaceConfig.COUNT;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple arg0) {
		if(!arg0.getSourceStreamId().equals(RaceConfig.RATIO_ID)){
			return;
		}
		payment = (PaymentMessage)arg0.getValue(0);
		createTime = (payment.getCreateTime()/1000/60)*60;
		platform = payment.getPayPlatform();
		platform_time = Short.toString(platform)+"_"+Long.toString(createTime);	
		price = payment.getPayAmount();
		if(map.containsKey(platform_time)){
//			b1=new BigDecimal(Double.toString(price));
//			b2=new BigDecimal(Double.toString(map.get(platform_time)));
//			result = b1.add(b2).doubleValue();
			result = price + map.get(platform_time);
			map.put(platform_time, result);
		}else{
			map.put(platform_time, price);
		}
		short i = 0;
		if(platform == (short)0){
			i=1;
			platform_time_other = Short.toString(i)+"_"+Long.toString(createTime);
			if(map.containsKey(platform_time_other)){
//				b1=new BigDecimal(Double.toString(map.get(platform_time))); 
//				b2=new BigDecimal(Double.toString(map.get(platform_time_other))); 
//				result = b2.divide(b1, 2, BigDecimal.ROUND_HALF_UP).doubleValue();
				result = map.get(platform_time_other)/map.get(platform_time);
			}else{
				result = 0;
			}
		}else if(platform == (short)1){
			i = 0;
			platform_time_other = Short.toString(i)+"_"+Long.toString(createTime);
			if(map.containsKey(platform_time_other)){
//				b1=new BigDecimal(Double.toString(map.get(platform_time))); 
//				b2=new BigDecimal(Double.toString(map.get(platform_time_other))); 
//				result = b1.divide(b2, 2, BigDecimal.ROUND_HALF_UP).doubleValue();
				result = map.get(platform_time)/map.get(platform_time_other);
				
			}else{
				result = 0;
			}
		}
		map1.put((String)("ratio_42853kwnfz_"+createTime), result);
	//	System.out.println(System.currentTimeMillis()+"--------------ratio_write_11----------------"+payment);
//		while(count-- >0){
//	//		System.out.println("ratio*******************"+count);
//			return;
//		}
//		count = RaceConfig.COUNT;
//		String[] string = map1.keySet().toArray(new String[0]);
//		for(String s: string){
//			double result = map1.get(s);
//			double result1 = Double.parseDouble(format.format(result));
//			TairOperatorImpl.write(s, (double)result1);
//			System.out.println("*******"+s+"   :"+ result1);
//			System.out.println(System.currentTimeMillis()+"--------------ratio_write----------------"+payment);
//		}
//		String s = "ratio_42853kwnfz_"+createTime;
//		System.out.println("*******"+s+"   :"+ result1);
//		collector.ack(arg0);
	}
	


	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
		startTime = System.currentTimeMillis();
		endTime = System.currentTimeMillis();
		new Thread(this).start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("Tair3"));
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
				Thread.sleep(RaceConfig.COUNT);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String[] string = map1.keySet().toArray(new String[0]);
			for(String s: string){
				double result = map1.get(s);
				double result1 = Double.parseDouble(format.format(result));
				TairOperatorImpl.write(s, (double)result1);
//				System.out.println("*******"+s+"   :"+ result1);
//				System.out.println(System.currentTimeMillis()+"--------------ratio_write----------------"+payment);
				
			}
		}
	}

}
