package com.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class MyClient implements Watcher{
	public static String url = "10.148.15.223:2181";
    private final static String root = "/myconf";
    // 数据库连接 URL， username passwd
    private final static String UrlNode = root + "/url";
    private final static String userNameNode = root + "/username";
    private final static String passWdNode = root + "/passwd";
    
    private final static String auth_type = "digest";
    private final static String auth_passwd = "ky123";
    
    private String aaa;
    private String bbb;
    private String ccc;
    
    private String ddd;

    ZooKeeper zk = null;

	public String getAaa() {
		return aaa;
	}

	public void setAaa(String aaa) {
		this.aaa = aaa;
	}

	public String getBbb() {
		return bbb;
	}

	public void setBbb(String bbb) {
		this.bbb = bbb;
	}

	public String getCcc() {
		return ccc;
	}

	public void setCcc(String ccc) {
		this.ccc = ccc;
	}

	public String getDdd() {
		return ddd;
	}

	public void setDdd(String ddd) {
		this.ddd = ddd;
	}

	public void initValue(){
		try{
			aaa = new String(zk.getData(UrlNode, true, null));
		} catch(Exception e){
			e.printStackTrace();
			aaa = null;
		}
		
		try{
			bbb = new String(zk.getData(userNameNode, true, null));
		} catch(Exception e){
			e.printStackTrace();
			bbb = null;
		}
		
		try{
			ccc = new String(zk.getData(passWdNode, true, null));
		} catch(Exception e){
			e.printStackTrace();
			ccc = null;
		}
		
		try{
			ddd = new String(zk.getData("/myconf/temp1006", true, null));
		} catch(Exception e){
			e.printStackTrace();
			ddd = null;
		}
	}
    
	public ZooKeeper getZK() throws Exception{
		zk = new ZooKeeper(url, 3000, this);
		zk.addAuthInfo(auth_type, auth_passwd.getBytes());
		while(zk.getState() != ZooKeeper.States.CONNECTED){
			Thread.sleep(3000);
		}
		return zk;
	}
	
	public static void main(String[] args) throws Exception {
		MyClient myClient = new MyClient();
		
		ZooKeeper zk = myClient.getZK();
		myClient.initValue();
		int i = 0;
		while(true){
			System.out.println(myClient.getAaa());
			System.out.println(myClient.getBbb());
			System.out.println(myClient.getCcc());
			System.out.println(myClient.getDdd());
			i++;
			System.out.println("******************************" + i);
			Thread.sleep(10000);
			
			if(i == 100){
				break;
			}
		}
		zk.close();
	}
	
	public void process(WatchedEvent event){
		if(event.getType() == Watcher.Event.EventType.None){
			System.out.println("连接服务器成功");
		} else if(event.getType() == Watcher.Event.EventType.NodeCreated){
			System.out.println("节点创建成功");
			// 读取新的配置
			initValue();
		} else if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged){
			System.out.println("子节点创建成功");
			// 读取新的配置
			initValue();
		} else if(event.getType() == Watcher.Event.EventType.NodeDataChanged){
			System.out.println("节点更新成功");
			// 读取新的配置
			initValue();
		} else if(event.getType() == Watcher.Event.EventType.NodeDeleted){
			System.out.println("节点删除成功");
			initValue();
		}
		
	}
}
