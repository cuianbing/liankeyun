package com.xian.liankloud.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
/*
          零售行业数据挖掘—购物篮分析
  一、购物篮分析——商品交易挖掘技术
  二、作用：1、帮助分析人员找出很有可能一起购买的商品。
           2、通过挖掘技术找到商品之间的相关性，将这些有关联规则的商品放在相邻的货架，方便顾客购买自己想要的商品。
  三、两个度量指标：支持度——一个项集的出现的频度。
                    置信度——关联规则左件与右件共同出现的频繁程度。
  四、利用购物篮的关联规则分析，可以为销售人员提供购物者的潜在购物行为：
        1.     哪些商品会一起够买？
        2.     每个购物篮里有哪些商品？
        3.     哪些商品应当降价销售？
        4.     商品应当如何相邻摆放可以实现最大利润？
        5.     如何确定电子商务网站的商品目录？
   五、测试交易数据
    a,b,c
    a,b,d
    b,c,d
    b,c,e
    b,d,e
    a,b
    a,c
    a,d
    b,d
    b,e
    c,d,e
    a,e
    b,d
    六、spark实现流程
      1、交易转换模式并找出频繁模式
      a,b,c ——> Map()     (a),1      a,b,d  ——> Map() (a),1    b,c ——> Map() (b),1   b,c ——>Map() (b),1
                          (b),1                       (b),1                  (c),1                (c),1
                          (c),1                       (d),1                  (b,c),1              (b,c),1
                          (a,b),1                     (a,b),1
                          (a,c),1                     (a,d),1
                          (b,c),1                     (b,d),1
                          (a,b,c),1                   (a,b,d),1

      ——> shuffle、sort、reduce()  (a),2
                                   (b),4
                                   (c),3
                                   (d),1
                                   (a,b),2
                                   (b,c),3
                                   (a,c),1
                                   (a,d),1
                                   (b,d),1
                                   (a,b,c),1
                                   (a,b,d),1
      2、生成关联规则
      频繁模式数据 ——> Map()生成子模式 ——> (a),2->(a),2
                                         (b),2->(b),4
                                         (c),3->(c),3
                                         (d),1->(d),4
                                         (a,d),2->[(a,b),(null,2)],[(a),((a,b),2)],[(a,b),2]
                                           ........
      ——> reduce()   ——>打印结果集
*
*
* */

object FindAssociationRules {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FindAssociationRules").setMaster("local[2]")
    sparkConf.set("spark.testing.memory", "471859200")
    val sc = new SparkContext(sparkConf)

    //文件输入流
    val input = "D:/data/shopping/input/inputshopping.txt"
    //文件输出流
    val output = "D:/data/shopping/output/shopping"
    val transactions = sc.textFile(input)

  //第一阶段生成频繁模式
    val patterns = transactions.flatMap(line => {
      val items = line.split(",").toList
      // Converting to List is required because Spark doesn't partition on Array (as returned by split method)
      (0 to items.size) flatMap items.combinations filter (xs => !xs.isEmpty)
      /*
        combinations(n: Int): Iterator[List[A]] 取列表中的n个元素进行组合，返回不重复的组合列表，结果一个迭代器
      * 即对a,b,c
      * 先取0个元素进行组合，得到不重复的组合列表[]，加入list中，list为[[]]
      * 再取1个元素进行组合，得到不重复的组合列表[[a],[b],[c]]，加入list中，list为[[a],[b],[c]]
      * 再取2个元素进行组合，得到不重复的组合列表[[a,b],[a,c],[b,c]]，加入list中,list为[[],[a],[b],[c],[a,b],[a,c],[b,c]]
      * 再取3个元素进行组合，得到不重复的组合列表[[a,b,c]]，加入list中,list为[[],[a],[b],[c],[a,b],[a,c],[b,c],[a,b,c]]
      * 然后对其进行过滤，去掉其中为空的列表
      * list为[[a],[b],[c],[a,b],[a,c],[b,c],[a,b,c]]
      * 最后回到外层的flatMap，会将列表的列表拍扁成列表：
      * [a],[b],[c],[a,b],[a,c],[b,c],[a,b,c]
      * */

    }).map((_, 1))


    //合并key值相同的键值对
    val combined = patterns.reduceByKey(_ + _)

    /*第二阶段在关联规则下，开始生成子模式
    给定一个频繁模式：(K=List<A1,A2,...,An>,V=Frequency)
    创建如下的子模式(K2,V2)
    (K2=K=List<A1,A2,...,An>,V2=Tuple(null,V))
    即把K作为K2，Tuple(null,V))作为V2
    (K2=List<A1,A2,...,An-1>),V2=Tuple(K,V))
    (K2=List<A1,A2,...,An-2,An>),V2=Tuple(K,V))
    ...
    (K2=List<A2,...,An-1,An>),V2=Tuple(K,V))
    即把K的每一个元素拿掉一次作为K2,Tuple(K,V))作为V2
    */
    val subpatterns = combined.flatMap(pattern => {
      //pattern:(List(a, b, c),1)
      val result = ListBuffer.empty[Tuple2[List[String], Tuple2[List[String], Int]]]
      result += ((pattern._1, (Nil, pattern._2)))//即把K作为K2，Tuple(null,V))作为V2
    //即每次去掉一个元素，将剩下的元素集合作为K2
      val sublist = for {
        i <- 0 until pattern._1.size
        xs = pattern._1.take(i) ++ pattern._1.drop(i + 1)
        if xs.size > 0
      } yield (xs, (pattern._1, pattern._2))
      result ++= sublist
      result.toList
    })
    val rules = subpatterns.groupByKey()
    //去空，聚合数据集
    val assocRules = rules.map(in => {
      println("in=" + in)
      //in:(List(b),CompactBuffer((List(),4), (List(b, d),1), (List(a, b),2), (List(b, c),3)))
      val fromCount = in._2.find(p => p._1 == Nil).get//找到[b]的frequency：即(List(),4)
      println("fromCount=" + fromCount)
      val toList = in._2.filter(p => p._1 != Nil).toList//将规则集合去掉空的
      println("toList=" + toList)
      //toList:CompactBuffer((List(b, d),1), (List(a, b),2), (List(b, c),3))
      if (toList.isEmpty) Nil
      else {
        val result =
          for {
            t2 <- toList
            confidence = t2._2.toDouble / fromCount._2.toDouble
            difference = t2._1 diff in._1
            //diff(that: collection.Seq[A]): List[A] 保存列表中那些不在另外一个列表中的元素，即从集合中减去与另外一个集合的交集
          } yield (((in._1, difference, confidence)))
        result
      }

    })

    assocRules.foreach(println)

    val formatResult = assocRules.flatMap(f => {
      //f.map(s => (s._1.mkString("[", ",", "]"), s._2.mkString("[", ",", "]"), s._3))
     f.map(s => (s._1.mkString, s._2.mkString, s._3))
    })
    formatResult.saveAsTextFile(output)

    sc.stop()
  }
}
