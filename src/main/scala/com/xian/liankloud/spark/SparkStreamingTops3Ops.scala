package com.xian.liankloud.spark

import java.util.Properties

import com.xian.liankloud.util.JDBCUtilsDemo1
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
建筑行业数据挖掘—商品房，经济房等排名计算
nc -lk 44446


001|apartment|shops
002|EconomicalHousing|apartment
003|shops|apartment
004|CheapHouses|OfficeBuilding
005|OfficeBuilding|shops
006|CheapHouses|apartment
007|OfficeBuilding|apartment
008|CheapHouses|shops
009|OfficeBuilding|apartment
*/

object SparkStreamingTops3Ops
{
  def main(args: Array[String]): Unit = {

    val sparkSession:SparkSession=SparkSession.builder().master("local[4]").appName("SparkStreamingTops3Ops")
      .enableHiveSupport()
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.kryoserializer.buffer", "256m")
    sparkSession.conf.set("spark.kryoserializer.buffer.max", "2046m")
    sparkSession.conf.set("spark.akka.frameSize", "500")
    sparkSession.conf.set("spark.rpc.askTimeout", "30")
    sparkSession.conf.set("spark.testing.memory", "471859200")

    import sparkSession.implicits._
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(2))

  /*val conf=new SparkConf().setMaster("local[2]").setAppName("SparkStreamingTops3Ops")
    conf.set("spark.testing.memory", "471859200")
  val ssc=new StreamingContext(conf,Seconds(2))
  -Dspark.testing.memory=1073741824
  */
  val sqlContext=new HiveContext(ssc.sparkContext)





    ssc.checkpoint("C:/data/ad-ck")

  val hostname="192.168.44.120"
  val port= 44445
  val linesDStream=ssc.socketTextStream(hostname,port)

  val pairRDD= linesDStream.map(line =>{
    val splits=line.split("\\|")
    val brand=splits(1)
    val category=splits(2)
    (category+"_"+ brand,1)
  })
  val rbkRDD=pairRDD.reduceByKey(_+_)
  val totalDStream:DStream[(String,Int)]=rbkRDD.updateStateByKey[Int]((curruent: Seq[Int], old: Option[Int])=>{
    var sum = curruent.sum
    if(old.isDefined){
      sum += old.get
    }
    Option(sum)

  })

  /*
  * 计算top3
  * */
  totalDStream.foreachRDD(rdd=>{
   if(!rdd.isEmpty()){
     val rowRDD:RDD[Row]=rdd.map(tuple=>{
       val category_brands=tuple._1.split("_")
       val category=category_brands(0)
       val brand=category_brands(1)
       val count=tuple._2
       Row(category,brand,count)

     })
     val schema=StructType(List(
       StructField("category",DataTypes.StringType,true),
       StructField("brand",DataTypes.StringType,true),
       StructField("count",DataTypes.IntegerType,true)
     ))
     val actionDF= sqlContext.createDataFrame(rowRDD,schema)
     actionDF.registerTempTable("user_action_count")
     val retDF=sqlContext.sql("SELECT "+
                                           "category, "+
                                           "brand, "+
                                           "count, "+
                                           "row_number() over(partition by category order by count desc) rank "+
                                    "FROM user_action_count " +
                                            "HAVING rank < 4")
     retDF.show()
     //val resultRowRDD = retDF.rdd

     //把DF变成RDD
     val resultRowRDD = retDF.rdd

     resultRowRDD.foreachPartition { partitionOfRecords => {
       //这里必须做非空判断，否则为null的时候，会报错，而且rdd不为null，partition可能为null，因为不能确保60秒中的每5秒都有数据
       if (partitionOfRecords.isEmpty) {
         println("This RDD is not null but partition is null")
       } else {
         // ConnectionPool is a static, lazily initialized pool of connections
         val connection = JDBCUtilsDemo1.getConnection()
         partitionOfRecords.foreach(record => {
           //把结果插入数据库中
           val sql = "insert into categorytop3(category,brand,count) values('" + record.getAs("category") + "','" +
             record.getAs("brand") + "'," + record.getAs("count") + ")"
           val stmt = connection.createStatement();
           stmt.executeUpdate(sql);
         })
         // JDBCUtilsDemo1.returnConnection(connection) // return to the pool for future reuse

       }
     }
     }
   }else{

   }

  })
  ssc.start()
  ssc.awaitTermination()
  }
}
