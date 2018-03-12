package com.demo;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.utils.DBUtils;

public class TransferDataFromMysql2Mongo {
	
	public long getUnixTempstamp(String dateStr) throws Exception {
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    Date date = sdf.parse(dateStr);
	    long tempstamp = date.getTime();
	    String unixDate = String.valueOf(tempstamp).substring(0, 10);
	    return Long.valueOf(unixDate);
	}
	
	public Date getDate(String dateStr) throws Exception {
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    Date date = sdf.parse(dateStr);
	    return date;
	}
	
	/**
	 * mysql中取出数据
	 */
	public List<Map<String, Object>> getDataFromMysql01() throws Exception {
	  String sql = "select summary_id,product_type,order_source,order_type,product_name,model,no_check_order,"
	   + "amount_check_order,quantity_check_order,no_payed_order,amount_payed_order,quantity_payed_order,start_dt,end_dt,load_dt,host "
	   + "from zzz_sum_order_by_minute where start_dt >='2016-03-01 00:00:00' and start_dt<='2016-07-27 23:59:59'"; 
	   PreparedStatement pst = DBUtils.getPreparedStatement(sql);
	   ResultSet rs = pst.executeQuery();
	   List<Map<String, Object>> list = DBUtils.resultSetToMapLs(rs); 
	   return list;
	}
	
	
	public void transfer() throws Exception {
		List<Map<String, Object>> list = getDataFromMysql01();
		MongoDB2 mongoDb = new MongoDB2("lemall_orders");
		int  i = 0;
		List<DBObject> dbObjects = new ArrayList<DBObject>();
		for(Map<String, Object> row : list){
			i++;
	    	DBObject o = new BasicDBObject();
	    	o.put("ts", getDate(row.get("start_dt") + ""));
	    	o.put("amount_check_order", Double.valueOf(row.get("amount_check_order") + "").longValue() * 100);
	    	o.put("quantity_check_order", Double.valueOf(row.get("quantity_check_order") + "").longValue() * 100);
	    	o.put("no_check_order", Double.valueOf(row.get("no_check_order") + "").longValue() * 100);
	    	o.put("amount_payed_order", Double.valueOf(row.get("amount_payed_order") + "").longValue() * 100);
	    	o.put("quantity_payed_order", Double.valueOf(row.get("quantity_payed_order") + "").longValue() * 100);
	    	o.put("no_payed_order", Double.valueOf(row.get("no_payed_order") + "").longValue() * 100);
	    	o.put("order_source_name", row.get("order_source"));
	    	o.put("order_type_name", row.get("order_type"));
	    	o.put("product_type_name", row.get("product_type"));
	    	o.put("sku_name", row.get("product_name"));
	    	dbObjects.add(o);
	    	if(i % 50000 == 0){
				System.out.println("==========================>" + i);
				mongoDb.insertBatch(dbObjects, "zzz_sum_order_by_minute");
				dbObjects = new ArrayList<DBObject>();
			}
		}
		mongoDb.insertBatch(dbObjects, "zzz_sum_order_by_minute");
		mongoDb.close();
	}
	
	public static void main(String[] args) throws Exception {
		new TransferDataFromMysql2Mongo().transfer();
	}
}