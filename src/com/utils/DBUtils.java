package com.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DBUtils {
    public static final String driverName = "com.mysql.jdbc.Driver";  
    public static final String user = "DW_M_SUMMARY";  
    public static final String password = "c07a21587f910b7";  
    public static final String url = "jdbc:mysql://10.150.120.37:3590/DW_M_SUMMARY?useUnicode=true&characterEncoding=utf8&autoReconnect=true&zeroDateTimeBehavior=convertToNull";  
  
    public static Connection conn = null;  
    public static PreparedStatement pst = null;  
  
    public static PreparedStatement getPreparedStatement(String sql) {  
        try {  
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
            pst = conn.prepareStatement(sql);
        } catch (Exception e) {  
            e.printStackTrace();  
        } 
        return pst;
    }  
  
    public static void close() {  
        try {  
        	pst.close();
            conn.close();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    } 
    
	public static List<Map<String, Object>> resultSetToMapLs(ResultSet rs) throws Exception{
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        Map<String, Object> map = null;
        ResultSetMetaData rsMeta = rs.getMetaData(); 
        while(rs.next()){
            map = new LinkedHashMap<String, Object>(rsMeta.getColumnCount());
            for(int i = 0; i < rsMeta.getColumnCount();){   
                String columnName = rsMeta.getColumnLabel(++i);  
                Object columnValue = rs.getObject(columnName);
                map.put(columnName, columnValue);
            } 
            list.add(map);
        }
        return list;
    }
}