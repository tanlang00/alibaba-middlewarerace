package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String Teamcode = "42853kwnfz";
    public static String JstormTopologyName = "42853kwnfz";
    public static String MetaConsumerGroup = "42853kwnfz";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
//    public static String TairConfigServer = "xxx";
//    public static String TairSalveConfigServer = "xxx";
    public static String TairConfigServer = "192.168.1.85:5198";
//    public static String TairConfigServer = "10.101.72.127:5198";
//    public static String TairSalveConfigServer = "10.101.72.128:5198";    
//    public static String TairGroup = "group_tianchi";
    public static String TairGroup = "group_1";
//    public static Integer TairNamespace = 14892;
    public static Integer TairNamespace = 0;
    public static String RocketMqServer = "192.168.1.107:9876";
    
    public static String TAOBAO_ID = "taobao_id";
    public static String TIANMAO_ID = "tianmao_id";
    public static String RATIO_ID = "ratio_id";
    
    public static final int COUNT = 10;
    public static final int COUNT_P = 1000;
}
