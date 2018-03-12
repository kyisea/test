package cn.itcast.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.itcast.constant.Constant;

/**
 * 将收到的原始商品名转换成大写在发送出去
 */
public class UpperBolt extends BaseBasicBolt{

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// 从tuple中拿到数据-原始商品名
		String productName = tuple.getStringByField(Constant.PRODUCT_NAME);
		System.out.println("***********************************************************************");
		System.out.println("**>" + productName + "**>" + tuple.getStringByField(Constant.PRODUCT_NAME2));
	    // 转换成大写
		String upper = productName.toUpperCase();
		// 发送出去
		collector.emit(new Values(new String[]{upper}));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[]{Constant.PRODUCT_NAME}));
	}

}
