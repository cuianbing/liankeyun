package com.xian.liankloud.conf;





import com.xian.liankloud.constants.Constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * 加载本地环境 测试环境 生产环境
 *
 */
public class ConfigurationManager {

    private ConfigurationManager() {}
    private static Properties properties =new Properties();
    /**
     * spark作业执行的模式
     * 有本地local
     * 有测试 test
     * 有生产 production
     */
    public static String mode = null;
    /**
     * 加载配置信息
     */
    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("conf.properties");
            properties.load(in);
            //获取spark作业运行的模式
            mode = properties.getProperty(Constants.SPARK_JOB_RUN_MODE, "local");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得字符串类的配置信息
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * 获得bollean类的配置信息
     * @param key
     * @return
     */
    public static boolean getBooleanProperty(String key) {
        return Boolean.valueOf(properties.getProperty(key).trim());
    }

    /**
     * 获得数字的配置信息
     * @param key
     * @return
     */
    public static int getIntProperty(String key) {
        return Integer.valueOf(properties.getProperty(key).trim());
    }

    public static long getLongProperty(String key) {
        return Long.valueOf(properties.getProperty(key).trim());
    }

  /*  public static void main(String[] args) {
        System.out.println("url= " + getProperty(Constants.JDBC_URL));
        System.out.println("maxActive= " + (getIntProperty(Constants.JDBC_MAX_ACTIVE) - 50));
        System.out.println("boolean= " + getBooleanProperty("test.boolean"));
    }*/
}
