package zhranklin

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Created by Zhranklin zhangwu@corp.netease.com at 2017/9/22
 */
object DataPreProcessors {
  val traceTypes = Set("POS消费", "车载消费")
  val consumeNot = Set("", "其他")
  def getConsumeStatRaw(filename: String)(implicit spark: SparkSession): Dataset[ConsumeRecord] = {
    import spark.implicits._
    spark.read.textFile(filename).cache()
      .map(_.replaceAll("\"", "").split(","))
      .filter(data ⇒ traceTypes.contains(data(1)) && !consumeNot.contains(data(3)))
      .map(data ⇒ ConsumeRecord(data(0).toInt, data(3), new Date(data(4)), (data(5).toDouble * 100).toInt, data(2)))
      .groupByKey(identity)
      .mapGroups((k, _) ⇒ k)
  }
}
