package com.xian.liankloud.constants;

/**
 * 用于存放项目开发过程中使用到的这种常量字段，避免手写导致出错
 *
 */
public interface Constants {
    /**
     * spark作业运行的模式
     */
    String SPARK_JOB_RUN_MODE = "spark.job.run.mode";
    /**
     * 关于数据库的常量字段
     */
    String JDBC_DRIVER_CLASS_NAME = "driverClassName";
    String JDBC_URL = "url";
    String JDBC_USERNAME = "username";
    String JDBC_PASSWORD = "password";
    /**
     * 当前数据库连接池的初始化大小
     */
    String JDBC_INITIAL_SIZE = "initialSize";
    /**
     * 当前数据库连接池的最大的活跃连接数据 默认：50
     */
    String JDBC_MAX_ACTIVE = "maxActive";
    /**
     * 当前数据库连接池的最大空闲数
     */
    String JDBC_MAX_IDLE = "maxIdle";
    /**
     * 当等待的时间
     */
    String JDBC_MAX_WAIT = "maxWait";


    //执行的spark作业的类型
    String SPARK_LOCAL_TASKID_SESSION = "spark.local.taskid.session";
    String SPARK_LOCAL_TASKID_PAGE = "spark.local.taskid.page";
    String SPARK_LOCAL_TASKID_PRODUCT = "spark.local.taskid.product";

    String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
    String KAFKA_TOPICS = "kafka.topics";

    /**
     * Spark作业相关的常量
     */
    String SPARK_APP_NAME_AD="SparkStreamingTops3Job";
    String SPARK_APP_NAME_SESSION = "UserSessionAggrStatisticsAnalysisApp";
    String SPARK_APP_NAME_PAGE = "PageOneStepConvertRateAPP";

    String FIELD_SESSION_ID = "sessionid";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_AGE = "age";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_START_TIME = "startTime";
    String FIELD_CLICK_COUNT = "clickCount";
    String FIELD_ORDER_COUNT = "orderCount";
    String FIELD_PAY_COUNT = "payCount";
    String FIELD_CATEGORY_ID = "categoryid";

    String SESSION_COUNT = "session_count";

    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";

    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";

    /**
     * 任务相关的常量
     */
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_SEX = "sex";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORY_IDS = "categoryIds";
    String PARAM_TARGET_PAGE_FLOW = "targetPageFlow";


}
