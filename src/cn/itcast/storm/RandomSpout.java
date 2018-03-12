package cn.itcast.storm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import cn.itcast.constant.Constant;

public class RandomSpout extends BaseRichSpout{
    
	SpoutOutputCollector collector = null;
	String[] goods = {"iphone", "xiaomi", "letv", "meizu", "zhongxing", "huawei", "moto", "samsung", "simens"};
	
    /**
     * 获取消息并发送给下一个组件的方法，会被storm不断地调用
     * 从goods数据中随机取一个商品名称封装到tuple中发送出去
     */
	public void nextTuple() {
		// 随机取到一个商品名称
		Random random = new Random();
		String good = goods[random.nextInt(goods.length)];
		// 封装到tuple中发送出去
		collector.emit(new Values(new String[]{good, "chelsea2222"}));
		
		Utils.sleep(500);
	}

	// 进行初始化，只在开始的时候调用一次
	public void open(Map conf, TopologyContext contest, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * 定义tuple的scheme
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[]{Constant.PRODUCT_NAME, Constant.PRODUCT_NAME2}));
	}

}