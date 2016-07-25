package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {
	
	static List<String> confServers;
	static DefaultTairManager tairManager;
	
	static{
		confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer);
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);
		tairManager.setGroupName(RaceConfig.TairGroup);
		tairManager.init();
	}
    public TairOperatorImpl() {
    }

    public static boolean write(Serializable key, Serializable value) {
    	ResultCode result = tairManager.put(RaceConfig.TairNamespace, key, value);
        return result.isSuccess();
    }

    public Object get(Serializable key) {
        return null;
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    }  
}
