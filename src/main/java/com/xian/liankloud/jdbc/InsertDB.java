package com.xian.liankloud.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Title: InsertDB.java
 * @Description:
 * @author: hangbin.zhang
 * @date 2018年9月6日
 * @version 1.0
 */
public class InsertDB {

    public static final String driver_class = "com.mysql.jdbc.Driver";
    public static final String driver_url = "jdbc:mysql://192.168.44.120/retail?useunicode=true&characterEncoding=utf8";
    public static final String user = "root";
    public static final String password = "123456";

    private static Connection conn = null;
    private PreparedStatement pst = null;
    private ResultSet rst = null;
    /**
     * Connection
     */
    public InsertDB() {
        try {
            conn = InsertDB.getConnInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 单例模式
     * 线程同步
     * @return
     */
    private static synchronized Connection getConnInstance() {
        if(conn == null){
            try {
                Class.forName(driver_class);
                conn = DriverManager.getConnection(driver_url, user, password);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println("连接数据库成功");
        }
        return conn;
    }


    /**
     * close
     */
    public void close() {

        try {
            if (conn != null) {
                InsertDB.conn.close();
            }
            if (pst != null) {
                this.pst.close();
            }
            if (rst != null) {
                this.rst.close();
            }
            System.out.println("关闭数据库成功");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * update
     *
     * @param sql
     * @param sqlValues
     * @return result
     * @throws SQLException
     */
    public int executeUpdate(String sql, List<String> sqlValues) throws SQLException {
        if(conn == null){
            try {
                conn = InsertDB.getConnInstance();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("数据库连接失败");
            }

        }
        conn.setAutoCommit(false);
        int result = -1;
        try {
            pst = conn.prepareStatement(sql);
            if (sqlValues != null && sqlValues.size() > 0) {
                setSqlValues(pst, sqlValues);
            }
            result = pst.executeUpdate();
            conn.commit();
            return result;
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
            e.printStackTrace();
        }
        return result;
    }

    public void saveData(String name1,String name2,double value) {
        StringBuffer sql = new StringBuffer();
        sql.append("INSERT INTO shop_basket ( Brand,Type,Value)")
                .append("VALUES (? , ? , ? ) ");
        List<String> sqlValues = new ArrayList<>();
        sqlValues.add(""+name1);
        sqlValues.add(""+name2);
        sqlValues.add(""+value);
        try {
            this.executeUpdate(sql.toString(), sqlValues);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * sql set value
     *
     * @param pst
     * @param sqlValues
     */
    private void setSqlValues(PreparedStatement pst, List<String> sqlValues) {
        for (int i = 0; i < sqlValues.size(); i++) {
            try {
                pst.setObject(i + 1, sqlValues.get(i));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
