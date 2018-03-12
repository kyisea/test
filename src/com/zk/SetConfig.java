package com.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class SetConfig {
    public static String url = "10.148.15.223:2181";
    private final static String root = "/myconf";
    // 数据库连接 URL， username passwd
    private final static String UrlNode = root + "/url";
    private final static String userNameNode = root + "/username";
    private final static String passWdNode = root + "/passwd";
    
    private final static String auth_type = "digest";
    private final static String auth_passwd = "ky123";
    
    private final static String URLString = "10.11.1.1";
    private final static String UserName = "username123";
    private final static String Passwd = "password123";
    
    public static void main(String[] args) throws Exception {
		ZooKeeper zk = new ZooKeeper(url, 3000, new Watcher(){
            public void process(WatchedEvent event){
            	System.out.println("触发了事件: " + event.getType());
            }    			
		});
		
		while(ZooKeeper.States.CONNECTED != zk.getState()){
			Thread.sleep(3000);;
		}
		zk.addAuthInfo(auth_type, auth_passwd.getBytes());
		
		if(zk.exists(root, true) == null){
			zk.create(root, "root".getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		if(zk.exists(UrlNode, true) == null){
			zk.create(UrlNode, URLString.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		if(zk.exists(userNameNode, true) == null){
			zk.create(userNameNode, UserName.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		if(zk.exists(passWdNode, true) == null){
			zk.create(passWdNode, Passwd.getBytes(), Ids.CREATOR_ALL_ACL,
					CreateMode.PERSISTENT);
		}
		
		zk.close();
	}
}


// ./zkCli.sh -server localhost:2181
// ls /myConf
// addauth digest ky123 /myconf
// get /myconf/url
