package com.xian.liankloud.util;



import com.xian.liankloud.conf.ConfigurationManager;
import com.xian.liankloud.constants.Constants;
import org.apache.commons.dbcp.BasicDataSourceFactory;


import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 关于DBCP和C3P0的说明
 * 这两个都是常见的数据库连接池技术
 */
public class DBCPUtil {
	private static DataSource ds;
	static {
		try {
			InputStream in = null;
			Properties properties = new Properties();
			/**
			 * 根据conf.properties中的配置信息来判断是执行本地 测试还是生产环境
			 */
			String runMode = properties.getProperty(Constants.SPARK_JOB_RUN_MODE, "local");
			if (runMode.equalsIgnoreCase("local")) {//加载本地开发资源环境
				in = ConfigurationManager.class.getClassLoader().getResourceAsStream("local/dbcp-config.properties");
			} else if(runMode.equalsIgnoreCase("test")) {//加载本地测试资源环境
				in = ConfigurationManager.class.getClassLoader().getResourceAsStream("test/dbcp-config.properties");
			} else {//加载生产资源环境
				in = ConfigurationManager.class.getClassLoader().getResourceAsStream("production/dbcp-config.properties");
			}
			properties.load(in);//加载相关的配置信息
			ds = BasicDataSourceFactory.createDataSource(properties);
		} catch (Exception e) {//初始化异常
			throw new ExceptionInInitializerError(e);
		}
	}
	
	public static DataSource getDataSource()
	{
		return ds;
	}
	
	public static Connection getConnection()
	{
		try {
			return ds.getConnection();
		} catch (SQLException e) {
			throw new ExceptionInInitializerError(e);
		}
	}


}
