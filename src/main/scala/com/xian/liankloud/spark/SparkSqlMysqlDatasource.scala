package com.xian.liankloud.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


/**
  * 零售行业案例预处理
  */
object SparkSqlMysqlDatasource
{

  //数据库配置
  lazy val url = "jdbc:mysql://192.168.44.120:3306/retail"
  lazy val username = "root"
  lazy val password = "123456"
 // System.setProperty("spark.sql.warehouse.dir","hdfs://HOSTNAME:9000/user/hive/warehouse/mydb1.db");
  def main(args: Array[String]) {
    //    val sparkConf = new SparkConf().setAppName("SparkSqlMysqlDatasource").setMaster("local[2]").set("spark.app.id", "sql")
  //  val sparkConf = new SparkConf().setAppName("SparkSqlMysqlDatasource")
     // .setMaster("spark://lky01:7077")
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val sparkSession:SparkSession=SparkSession.builder().master("local[4]").appName("SparkSqlMysqlDatasource")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
      .enableHiveSupport()
      .getOrCreate()

    //序列化
     sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.kryoserializer.buffer", "256m")
    sparkSession.conf.set("spark.kryoserializer.buffer.max", "2046m")
    sparkSession.conf.set("spark.akka.frameSize", "500")
    sparkSession.conf.set("spark.rpc.askTimeout", "30")
    sparkSession.conf.set("spark.testing.memory", "471859200")

    //获取context
    // val sc = new SparkContext(sparkConf)


    //获取sqlContext
    //val sqlContext = new SQLContext(sc)
    val sqlContext:SQLContext=sparkSession.sqlContext
    //引入隐式转换，可以使用spark sql内置函数
    import sqlContext._
    //创建jdbc连接信息
    val uri = url + "?user=" + username + "&password=" + password + "&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    val prop = new Properties()
    //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
    prop.put("driver", "com.mysql.jdbc.Driver")
    //加载mysql数据表
    val df_test1: DataFrame = sparkSession.read.jdbc(uri, "consume_notes", prop)
    val df_test2: DataFrame = sparkSession.read.jdbc(uri, "hykxx", prop)

    //从dataframe中获取所需字段

    //注册成临时表
    df_test1.registerTempTable("consume_notes_test")
    df_test2.registerTempTable("hykxx_test")

  val total_sql=" SELECT cnt.sf AS sf,cnt.age AS age,cnt.RFMmc AS RFMmc,cnt.VIPnumber AS VIPnumber,cnt.consume_day AS consume_day, cnt.consume_month AS consume_month,cnt.consume_year AS consume_year, cnt.cpdlmc AS cpdlmc,cnt.cpflmc AS cpflmc,cnt.cpmc AS cpmc,cnt.cpxlmc AS cpxlmc, htt.cpztmc AS cpztmc,htt.dkhbs AS dkhbs, htt.hy_no AS hy_no,htt.hyklxmc AS hyklxmc,cnt.hysmzq AS hysmzq, cnt.jf AS jf,cnt.jkrq AS jkrq, cnt.klxmc AS klxmc,cnt.sbmc AS sbmc, cnt.sex AS sex, cnt.xfje AS xfje,cnt.xfls AS xfls, cnt.xfsj AS xfsj, cnt.xfsl AS xfsl,cnt.zkje AS zkje FROM consume_notes_test cnt,hykxx_test htt WHERE cnt.hy_no=htt.hy_no"
    val total_df: DataFrame = sparkSession.sql(total_sql)

    //将结果写入数据库中
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    total_df.write.mode("append").jdbc("jdbc:mysql://192.168.44.120:3306/retail?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull","retail_hy",properties)

    //停止SparkContext
    sparkSession.stop()


  }
}
