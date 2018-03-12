package cn.itcast.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * 描述topology的结构，以及创建topology并提交给集群
 */
public class TopoMain {
    
	public static void main(String[] args) throws Exception {
	    TopologyBuilder topologyBuilder	= new TopologyBuilder();
	    
	    // 设置消息源组件为Randomspout
	    topologyBuilder.setSpout("randowspout", new RandomSpout(), new Integer(4));
	    
	    // 设置逻辑处理组件upperbolt,并指定接收randowspout的消息
	    topologyBuilder.setBolt("upperbolt", new UpperBolt(), new Integer(4)).shuffleGrouping("randowspout");
	    
	    // 设置逻辑处理组件suffixbolt,并指定接收upperbolt的消息
	    topologyBuilder.setBolt("suffixbolt", new SuffixBolt(), new Integer(4)).shuffleGrouping("upperbolt");
	    
	    // 创建一个topology
	    StormTopology topo = topologyBuilder.createTopology();
	    
	    // 创建一个storm的配置参数对象
	    Config conf = new Config();
	    conf.setNumWorkers(4); // 设置storm集群为这个topo启动的进程数
	    conf.setDebug(true);
	    conf.setNumAckers(0);
	    
	    // 提交topo到storm集群中
	    StormSubmitter.submitTopology("demotopo", conf, topo);
	}
}