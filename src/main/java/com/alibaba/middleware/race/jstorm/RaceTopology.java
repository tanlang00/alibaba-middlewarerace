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
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 1;
        int count_Parallelism_hint = 1;
        int count_workers = 4;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RockMqSpout(), spout_Parallelism_hint);
//        builder.setSpout("spout3", new RatioSpout(), spout_Parallelism_hint);
//        builder.setSpout("spout1", new TaoBaoSpout(), spout_Parallelism_hint);
//        builder.setSpout("spout2", new TianMaoSpout(), spout_Parallelism_hint);
        PreparedBolt b = new PreparedBolt();
//        builder.setBolt("bolt1_1", b,count_Parallelism_hint).shuffleGrouping("spout",RaceConfig.TAOBAO_ID);
//        builder.setBolt("bolt1_2", b,count_Parallelism_hint).shuffleGrouping("spout",RaceConfig.TIANMAO_ID);
        builder.setBolt("bolt1_3", b,count_Parallelism_hint).shuffleGrouping("spout",RaceConfig.RATIO_ID);
        builder.setBolt("bolt2", new CalTTBolt(),split_Parallelism_hint).shuffleGrouping("bolt1_1");
        builder.setBolt("bolt3", new CalTTBolt(),split_Parallelism_hint).shuffleGrouping("bolt1_2");
        builder.setBolt("bolt4", new CalTTBolt(),split_Parallelism_hint).shuffleGrouping("bolt1_3");
        builder.setBolt("bolt5", new RatioBolt(),split_Parallelism_hint).shuffleGrouping("spout",RaceConfig.RATIO_ID);
//        builder.setSpout("spout1", new RatioSpout(),1);
//        builder.setBolt("bolt", new RatioBolt(),6).shuffleGrouping("spout1",RaceConfig.RATIO_ID);
        String topologyName = RaceConfig.JstormTopologyName;
        
        Config.setNumAckers(conf, 0);
        conf.put(Config.TOPOLOGY_WORKERS, count_workers);
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        
        
        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        	LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology(topologyName, conf, builder.createTopology());
//        	Thread.sleep(30000);
//        	cluster.shutdown();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
