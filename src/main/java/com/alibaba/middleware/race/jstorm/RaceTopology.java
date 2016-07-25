package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.alibaba.middleware.race.RaceConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setDebug(false);
        int spout_Parallelism_hint = 4;
        int split_Parallelism_hint = 2;
        int count_Parallelism_hint = 2;
        int count_workers = 4;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RockMqSpout(), spout_Parallelism_hint);
          //分属于不同worker，导致数据处理失败，数据已经分流了，bolt中的逻辑无法完成
          //如要修改，则应该将preparedBolt集中到一个worker中，保证其中共享变量有效，则应该采用一个bolt，多个线程，就不会造成错误。
        builder.setBolt("bolt1_1", new PreparedBolt(),count_Parallelism_hint).shuffleGrouping("spout",RaceConfig.TAOBAO_ID);
        builder.setBolt("bolt1_2", new PreparedBolt(),count_Parallelism_hint).shuffleGrouping("spout",RaceConfig.TIANMAO_ID);
        builder.setBolt("bolt1_3", new PreparedBolt(),count_Parallelism_hint).shuffleGrouping("spout",RaceConfig.RATIO_ID);
        builder.setBolt("bolt2", new CalTTBolt(),split_Parallelism_hint).shuffleGrouping("bolt1_1");
        builder.setBolt("bolt3", new CalTTBolt(),split_Parallelism_hint).shuffleGrouping("bolt1_2");
        builder.setBolt("bolt4", new CalTTBolt(),split_Parallelism_hint).shuffleGrouping("bolt1_3");
        builder.setBolt("bolt5", new RatioBolt(),3).shuffleGrouping("spout",RaceConfig.RATIO_ID);
        String topologyName = RaceConfig.JstormTopologyName;
        
        Config.setNumAckers(conf, 0);
        conf.put(Config.TOPOLOGY_WORKERS, count_workers);
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        
        
        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        //	LocalCluster cluster = new LocalCluster();
        //	cluster.submitTopology(topologyName, conf, builder.createTopology());
//        	Thread.sleep(30000);
//        	cluster.shutdown();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
