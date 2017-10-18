package zhranklin

import java.util.Date

import org.apache.spark.sql.SparkSession

object ConsumeByTime {
  case class Stat(consume: String, date: String, count: Int, totalCost: Double)
  def main(args: Array[String]) {
    implicit val spark: SparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._
    DataPreProcessors.getConsumeStatRaw(args(0))
      .groupByKey(record ⇒ (record.timestamp / MILLIS_PER_DAY, record.consume))
      .mapGroups {
        case ((_, consume), recordItr) ⇒
          val records = recordItr.toList
          val d: Date = records.head.timestamp
          val dateStr = f"$d%tY/$d%tm/$d%td"
          (Stat(consume, dateStr, records.size, records.map(_.cost).sum.toDouble / 100), (consume, dateStr))
      }
      .sort("_2")
      .map{tp ⇒ import tp._1._; s"$consume,$date,$count,$totalCost"}
      .write.text(args(1))
    spark.stop()
  }
}