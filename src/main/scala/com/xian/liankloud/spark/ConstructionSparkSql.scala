package com.xian.liankloud.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/*
* 建筑预处理
* */
object ConstructionSparkSql
{
  //数据库配置
  lazy val url = "jdbc:mysql://192.168.44.120:3306/construction"
  lazy val username = "root"
  lazy val password = "123456"
  // System.setProperty("spark.sql.warehouse.dir","hdfs://HOSTNAME:9000/user/hive/warehouse/mydb1.db");
  def main(args: Array[String]) {
    //    val sparkConf = new SparkConf().setAppName("SparkSqlMysqlDatasource").setMaster("local[2]").set("spark.app.id", "sql")
    //  val sparkConf = new SparkConf().setAppName("SparkSqlMysqlDatasource")
    // .setMaster("spark://lky01:7077")
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val sparkSession:SparkSession=SparkSession.builder().master("local[2]").appName("SparkSqlMysqlDatasource")
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
    //val df_test1: DataFrame = sparkSession.read.jdbc(uri, "cwzbjl", prop)
    //val df_test2: DataFrame = sparkSession.read.jdbc(uri, "product", prop)
    val df_test3: DataFrame = sparkSession.read.jdbc(uri, "company", prop)
    val df_test4: DataFrame = sparkSession.read.jdbc(uri,"subject_table", prop)
    val df_test5: DataFrame = sparkSession.read.jdbc(uri,"task",prop)
    //val df_test6: DataFrame = sparkSession.read.jdbc(uri, "sczbjl", prop)
    //val df_test7: DataFrame = sparkSession.read.jdbc(uri, "investigate", prop)
    val df_test8: DataFrame = sparkSession.read.jdbc(uri, "project", prop)
    //val df_test9: DataFrame = sparkSession.read.jdbc(uri,"project_subject", prop)
    val df_test10: DataFrame = sparkSession.read.jdbc(uri,"ywzbjl",prop)
    //从dataframe中获取所需字段

    //注册成临时表
    //df_test1.registerTempTable("cwzbjl_test")
    //df_test2.registerTempTable("product_test")
    df_test3.registerTempTable("company_test")
   df_test4.registerTempTable("subject_table_test")
    df_test5.registerTempTable("task_test")
    //df_test6.registerTempTable("sczbjl_test")
   // df_test7.registerTempTable("investigate_test")
    df_test8.registerTempTable("project_test")
    //df_test9.registerTempTable("project_subject_test")
    df_test10.registerTempTable("ywzbjl_test")

    //val total_sql="SELECT  cwz.ID as ID,cwz.gsID as gsID,cwz.xmkmID as xmkmID,cwz.bbrq as bbrq,cwz.qms as qms,cwz.ncs as ncs,cwz.bqs as bqs,cwz.ysz as ysz,cwz.hkl as hkl,cwz.ts as ts,cwz.area as area,cwz.qyjg as qyjg,cwz.hkjg as hkjg,cwz.jj as jj,pro.cpID as cpID,pro.cpmc as cpmc,com.gsmc as gsmc,com.jtzs as jtzs,sub.kmID as kmID,sub.kmmc as kmmc,tas.rwID as rwID,tas.rwmc as rwmc,tas.jhksrq as jhksrq,tas.jhjsrq as jhjsrq,tas.sjwgrq as sjwgrq,tas.wcl as wcl,scz.number as number,scz.bgsj as bgsj,scz.dcsj as dcsj,scz.myd as myd,scz.hybg as hybg,scz.cpfhd as cpfhd,scz.ppmbwcl as ppmbwcl,inv.dczb as dczb,inv.dcwd as dcwd,inv.dcxm as dcxm,inv.dcxmms as dcxmms,pjt.xmmc as xmmc,pjt.ssxm as ssxm,pjt.ssqy as ssqy,pjt.xmsx as xmsx,pjt.wysx as wysx,pjt.zlgyssx as zlgyssx,pst.xmkmmc as xmkmmc,ywz.gyszb as gyszb,ywz.qdzbtzssj as qdzbtzssj,ywz.sckpsj as sckpsj,ywz.kpsj as kpsj,ywz.qxs as qxs,ywz.jfrq as jfrq,ywz.jfl as jfl,ywz.cbys as cbys,ywz.tzsj as tzsj FROM cwzbjl_test cwz,product_test pro,company_test com,subject_table_test  sub,task_test tas,sczbjl_test scz,investigate_test inv,project_test pjt,project_subject_test pst,ywzbjl_test ywz WHERE   cwz.gsID=com.gsID AND pst.xmkmID=cwz.xmkmID AND scz.gsID=cwz.gsID AND ywz.gsID=cwz.gsID AND scz.cpID=pro.cpID AND scz.number=inv.number AND scz.xmID=pjt.xmID AND ywz.kmID=sub.kmID AND ywz.rwID=tas.rwID order by cwz.ID=10"
    //市场指标
    //val total_sql="select scz.number as number,scz.bgsj as bgsj,scz.dcsj as dcsj,scz.myd as myd,scz.hybg as hybg,scz.cpfhd as cpfhd,scz.ppmbwcl as ppmbwcl,pro.cpID as cpID,pro.cpmc as cpmc,inv.dczb as dczb,inv.dcwd as dcwd,inv.dcxm as dcxm,inv.dcxmms as dcxmms,pjt.xmmc as xmmc,pjt.ssxm as ssxm,pjt.ssqy as ssqy,pjt.xmsx as xmsx,pjt.wysx as wysx,pjt.zlgyssx as zlgyssx,pjt.sf as sf,com.gsmc as gsmc,com.jtzs as jtzs from product_test pro,company_test com,investigate_test inv,project_test pjt,sczbjl_test scz where scz.cpID=pro.cpID and scz.number=inv.number and scz.xmID=pjt.xmID and scz.gsID=com.gsID"
    //业务指标
    val total_sql="select ywz.gyszb as gyszb,ywz.qdzbtzssj as qdzbtzssj,ywz.sckpsj as sckpsj,ywz.kpsj as kpsj,ywz.qxs as qxs,ywz.jfrq as jfrq,ywz.jfl as jfl,ywz.cbys as cbys,ywz.tzsj as tzsj,sub.kmID as kmID,sub.kmmc as kmmc,tas.rwID as rwID,tas.rwmc as rwmc,tas.jhksrq as jhksrq,tas.jhjsrq as jhjsrq,tas.sjwgrq as sjwgrq,tas.wcl as wcl,com.gsmc as gsmc,com.jtzs as jtzs,com.gsID as gsID,pjt.xmID as xmID,pjt.xmmc as xmmc,pjt.ssxm as ssxm,pjt.ssqy as ssqy,pjt.xmsx as xmsx,pjt.wysx as wysx,pjt.zlgyssx as zlgyssx,pjt.sf as sf from subject_table_test sub,task_test tas,ywzbjl_test ywz,project_test pjt,company_test com where ywz.kmID=sub.kmID  and ywz.rwID=tas.rwID and ywz.gsID=com.gsID and ywz.xmID=pjt.xmID"

    val total_df: DataFrame = sparkSession.sql(total_sql)

    //将结果写入数据库中
    val properties=new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    //total_df.write.mode("append").jdbc("jdbc:mysql://192.168.0.200:3306/construct?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull","market_index",properties)
      total_df.write.mode("append").jdbc("jdbc:mysql://192.168.44.120:3306/construct?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull","operational_indicator",properties)
    //停止SparkContext
    sparkSession.stop()


  }
}
