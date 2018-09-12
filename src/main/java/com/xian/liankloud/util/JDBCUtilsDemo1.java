package com.xian.liankloud.util;



/**
 * Created by Administrator on 2018/9/6.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
public class JDBCUtilsDemo1{

    static{
        /**
         *1. 加载驱动
         */
        try {
            Class.forName("com.mysql.jdbc.Driver");    //利用反射加载驱动
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    /**
     * 2.建立连接
     */
    public static Connection getConnection() {
        try {
            return DriverManager.getConnection("jdbc:mysql://192.168.44.120:3306/construct?characterEncoding=utf-8","root", "123456");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * 关闭连接
     * @param conn
     * @param prep
     * @param rs
     */
    public static void close(Connection conn,PreparedStatement prep,ResultSet rs){
        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally{
                rs = null;
            }
        }
        if(prep != null){
            try {
                prep.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally{
                prep = null;
            }
        }
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally{
                conn = null;
            }
        }
    }
    /**
     * 测试数据库连通性
     */
    public static void main(String[] args) {
        //调取连接数据库函数
        Connection conn=JDBCUtilsDemo1.getConnection();
        System.out.println(conn);
        //关闭数据库
        JDBCUtilsDemo1.close(conn,null, null);
    }
}
