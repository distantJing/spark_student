package org.wq.scala.ml

import java.io.FileOutputStream

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/10/24.
  */
object FP_GrowthTest {
  val cardfile = "C:/IdeaProjects/untitled2/student_source_abstract.txt"
  val fpResult = new FileOutputStream("C:/IdeaProjects/untitled2/fpGrowth_Result.txt")
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("FPGrowthTest").setMaster("local")
      //.set("spark.sql.warehouse.dir","C:/IdeaProjects/untitled2/fp_source.txt")
    val sc = new SparkContext(conf)
    //设置参数
    //最小支持度
    val minSupport=0.05
    //最小置信度
    val minConfidence=0.5
    //数据分区
    val numPartitions=4

    //取出数据
    val data = sc.textFile(cardfile)

    //把数据通过空格分割
    val transactions=data.map(x=>x.split(" "))
    transactions.cache()
    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()
    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    //把数据带入算法中
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    model.freqItemsets.collect().foreach(itemset=>{
      val content = ((itemset.items.mkString("[", ",", "]")+","+itemset.freq).toString + "\r\n").getBytes()
      // println( itemset.items.mkString("[", ",", "]")+","+itemset.freq)
      fpResult.write(content)
    })

    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //这里可以把规则写入到Mysql数据库中，以后使用来做推荐
    //如果规则过多就把规则写入redis，这里就可以直接从内存中读取了，我选择的方式是写入Mysql，然后再把推荐清单写入redis
    model.generateAssociationRules(minConfidence).collect().foreach(rule=>{
      val temp = rule.antecedent.mkString(",")+"-->"+ rule.consequent.mkString(",")+"-->"+ rule.confidence
      println(rule.antecedent.mkString(",")+"-->"+ rule.consequent.mkString(",")+"-->"+ rule.confidence)
      val content = (temp.toString + "\r\n").getBytes()
      fpResult.write(content)
    })
    //查看规则生成的数量
    println(model.generateAssociationRules(minConfidence).collect().length)

    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

    fpResult.close()
  }
}