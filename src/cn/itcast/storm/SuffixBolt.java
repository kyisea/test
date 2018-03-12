package cn.itcast.storm;

import java.io.FileWriter;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.itcast.constant.Constant;

/**
 * 给商品名添加后缀，然后写入到文件中
 *
 */
public class SuffixBolt extends BaseBasicBolt{
	FileWriter fileWriter = null;
	
	public void prepare(Map stormConf, TopologyContext context) {
		try{
		    fileWriter = new FileWriter("/home/kangyue/output/" + UUID.randomUUID());
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// 从消息元组tuple中拿到上一个组件发送过来的数据
		String productName = tuple.getStringByField(Constant.PRODUCT_NAME);
		System.out.println("#########################################################################33");
		System.out.println("##>" + productName);
	    // 给商品名称添加后缀
		String result = productName + "_suffix";
		// 写入到文件
		try{
			fileWriter.append(result);
			fileWriter.append("\n");
			fileWriter.flush();
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	// 声明该组件要发送出去的tuple的字段定义
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}