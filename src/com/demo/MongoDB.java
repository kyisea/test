package com.demo;

import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDB {
	// 1 建立一个Mongo的数据库连接对象
	static Mongo connection = null;
	// 2 创建相关数据库的连接
	static DB db = null;
	
	public MongoDB(String dataBaseName){
		connection = new Mongo("10.148.15.223:27017");
		db = connection.getDB(dataBaseName);
	}
	
    public static void main(String[] args) {
		// 实例化
    	MongoDB mongoDb = new MongoDB("foobar");
    	
    	/**
    	 * 1 创建集合
    	 */
//    	mongoDb.createCollection("javadb");
    	
    	/**
    	 * 2 添加数据
    	 */
//    	DBObject dbs = new BasicDBObject();
//    	dbs.put("name", "uspcat.com");
//    	dbs.put("age", 2);
//    	List<String> books = new ArrayList<String>();
//    	books.add("EXTJS");
//    	books.add("MONGODB");
//    	dbs.put("books", books);
//    	mongoDb.insert(dbs, "javadb");
    	
    	/**
    	 * 3 批量插入数据
    	 */
//    	List<DBObject> dbObjects = new ArrayList<DBObject>();
//    	DBObject jom = new BasicDBObject("name","jim");
//    	DBObject lisi = new BasicDBObject("name","lisi");
//    	dbObjects.add(jom);
//    	dbObjects.add(lisi);
//    	mongoDb.insertBatch(dbObjects, "javadb");
    	
    	/**
    	 * 4 删除 
    	 */
//    	mongoDb.deleteById("57c2607bac92371eec246356", "javadb");
    	
    	/**
    	 * 5 根据条件删除数据
    	 */
//		DBObject lisi = new BasicDBObject();
//		lisi.put("name", "lisi");
//		mongoDb.deleteByDbs(lisi, "javadb");
    	
		/**
		 * 6.更新操作,为集合增加email属性
		 */
//		DBObject update = new BasicDBObject();
//		update.put("$set",  new BasicDBObject("age",1));
//		mongoDb.update(new BasicDBObject(), update,false,true,"javadb");
    	
		/**
		 * 7.查询出persons集合中的name和age
		 */
//		DBObject keys = new BasicDBObject();
//		keys.put("_id", false);
//		keys.put("name", true);
//		keys.put("age", true);
//		DBCursor cursor = mongoDb.find(null, keys, "javadb");
//	    while (cursor.hasNext()) {
//		    DBObject object = cursor.next();
//		    System.out.println(object.get("name"));
//		    System.out.println(object.get("age"));
//		    System.out.println(object.get("eamil"));
//	    }
    	
		/**
		 * 8.分页例子
		 */
		DBCursor cursor = mongoDb.find(null, null, 1, 3, "persons");
	    while (cursor.hasNext()) {
		    DBObject object = cursor.next();
		    System.out.print(object.get("name")+"-->");
		    System.out.print(object.get("age")+"-->");
		    System.out.println(object.get("e"));
	    }		
		//关闭连接对象
		connection.close();
	}
    
    public void createCollection(String collname){
    	DBObject dbs = new BasicDBObject();
    	db.createCollection(collname, dbs);
    }
    
    public void insert(DBObject dbs, String collname){
    	DBCollection coll = db.getCollection(collname);
        coll.insert(dbs);
    }
    
    public void insertBatch(List<DBObject> dbsets, String collname){
    	DBCollection coll = db.getCollection(collname);
        coll.insert(dbsets);
    }
    
    public void deleteById(String id, String collname){
    	DBCollection coll = db.getCollection(collname);
    	DBObject dbs = new BasicDBObject("_id", new ObjectId(id));
        coll.remove(dbs);
    }
    
	public void deleteByDbs(DBObject dbs,String collName){
		DBCollection coll = db.getCollection(collName);
		coll.remove(dbs);
	}
	
	public void update(DBObject find, DBObject update, boolean upsert,
			boolean multi, String collName) {
		// 1.得到集合
		DBCollection coll = db.getCollection(collName);
		coll.update(find, update, upsert, multi);
	}
	
	public DBCursor find(DBObject ref,
			DBObject keys,
			String collName){
		//1.得到集合
		DBCollection coll = db.getCollection(collName);
		DBCursor cur = coll.find(ref, keys);
		return cur;
	}	
	
	public DBCursor find(DBObject ref, 
			DBObject keys,
			int start,
			int limit,
			String collName){
		DBCursor cur = find(ref, keys, collName);
		return cur.limit(limit).skip(start);
	}
}