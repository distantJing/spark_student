package zhranklin

import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Zhranklin zhangwu@corp.netease.com at 2017/9/22
  */
//9443,"POS消费","地点87","食堂","2015/04/24 12:11:59","4.4","46.8"
object DataPreProcessors {
  // consume
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

// library
  val libraryPlaceNot = Set("null", "其他")
  def getLibraryStatRaw(filename: String)(implicit  spark: SparkSession): Dataset[LibraryRecord] = {
    import spark.implicits._
    spark.read.textFile(filename).cache()
      .map(_.replaceAll("\"", "").split(","))
      .filter(data ⇒ !libraryPlaceNot.contains(data(1)))
      .map(data ⇒ LibraryRecord(data(0).toInt, data(1).toString, new Date(data(2))))
      .groupByKey(identity)
      .mapGroups((k, _) => k)
  }


  // dorm
  def getDormStatRaw(filename: String)(implicit  spark: SparkSession): Dataset[DormRecord] = {
    import spark.implicits._
    spark.read.textFile(filename).cache()
      .map(_.replaceAll("\"", "").split(","))
      .map(data ⇒ DormRecord(data(0).toInt, new Date(data(1)), data(2).toInt))
      .groupByKey(identity)
      .mapGroups((k, _) => k)
  }

  // score
  def getScoreStatRaw(filename: String)(implicit  spark: SparkSession): Dataset[ScoreRecord] = {
    import spark.implicits._
    spark.read.textFile(filename).cache()
      .map(_.split(","))
      .map(data ⇒ ScoreRecord(data(0).toInt, data(1).toInt, data(2).toInt))
      .groupByKey(identity)
      .mapGroups((k, _) => k)
  }
}
