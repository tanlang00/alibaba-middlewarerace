package com.alibaba.middleware.race.Tair;

import java.io.Serializable;

public class Fengzhuang implements Serializable{
	
	private static final long serialVersionUID = -5813016688884666789L;
	public double price ;
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public int platform ;
	public int getPlatform() {
		return platform;
	}
	public void setPlatform(int platform) {
		this.platform = platform;
	}
	
	public boolean subprice(double value){
		this.price = this.price - value;
		if(price == 0){
			return true;
		}
		return false;
	}
}
