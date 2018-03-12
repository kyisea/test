package com.demo;

import java.util.List;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class DataBase {
     public static void main(String[] args) {
		// 1 建立一个Mongo的数据库连接对象
    	Mongo mongo = new Mongo("10.148.15.224:27017");
    	// 2  查询所有的databasename
    	List<String> dbName = mongo.getDatabaseNames();
    	for(String name:dbName){
    		System.out.println(name);
    	}
    	System.out.println("********************");
    	// 3 创建相关数据库(foobar)的连接
    	DB db = mongo.getDB("foobar");
    	// 4 查询数据库所有集合名字
    	Set<String> collNames = db.getCollectionNames();
    	for(String name  : collNames){
    		System.out.println(name);
    	}
    	System.out.println("********************");
    	// 5 查询所有的数据
    	DBCollection users = db.getCollection("persons");
    	DBCursor cur = users.find();
    	while(cur.hasNext()){
    		DBObject object = cur.next();
    		System.out.println(object.get("name"));
    	}
    	System.out.println("********************");
    	// 6 其他操作
    	System.out.println(cur.count());
    	System.out.println(JSON.serialize(cur));
	}
}