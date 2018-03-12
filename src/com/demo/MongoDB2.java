package com.demo;

import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDB2 {
	// 1 建立一个Mongo的数据库连接对象
	static Mongo connection = null;
	// 2 创建相关数据库的连接
	static DB db = null;
	
	public MongoDB2(String dataBaseName){
		connection = new Mongo("10.148.15.223:27017");
		db = connection.getDB(dataBaseName);
	}
    
    public void insertBatch(List<DBObject> dbsets, String collname){
    	DBCollection coll = db.getCollection(collname);
        coll.insert(dbsets);
    }
    
    public void close(){
    	connection.close();
    }
}