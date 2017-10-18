package zhranklin
import org.apache.spark.sql.{Dataset, SparkSession}

object ConsumeByStudent {
  case class StatRaw(student: Int, consume: String, count: Int, totalCost: Int, minTime: Long, maxTime: Long)
  case class Stat(student: Int, consume: String, countDaily: Double, costDaily: Double)
  def main(args: Array[String]) {
    implicit val spark: SparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._
    DataPreProcessors.getConsumeStatRaw(args(0))
      .groupByKey(record ⇒ (record.student, record.consume))
      .mapGroups {
        case ((student, consume), recordItr) ⇒
          val records = recordItr.toList
          val (count, totalCost) = records.foldLeft((0, 0)) {
            case ((count0, totalCost0), record) ⇒ (count0 + 1, totalCost0 + record.cost)
          }
          StatRaw(student, consume, count, totalCost, records.map(_.timestamp).min, records.map(_.timestamp).max)
      }
      .groupByKey(_.student)
      .mapGroups{ (stu, recordItr) ⇒
        val records = recordItr.toList
        val (countSum, totalCostSum) = records.foldLeft((0, 0)) {
          case ((countSum0, totalCostSum0), stat) ⇒ (countSum0 + stat.count, totalCostSum0 + stat.totalCost)
        }
        val maxTime = records.map(_.maxTime).max
        val minTime = records.map(_.minTime).min
        val duringDate = (maxTime - minTime - 1) / 86400000 + 1
        val results = (records.sortBy(_.consume) :+ StatRaw(stu, "总计", countSum, totalCostSum, 0, 0))
          .map(raw ⇒ Stat(raw.student, raw.consume, raw.count.toDouble / duringDate, raw.totalCost.toDouble / duringDate / 100))
        (results, totalCostSum.toDouble / 100)
      }
      .sort($"_2".desc)
      .map {
        case (stats, total) ⇒
          f"${stats.head.student}%s,$total%.2f," +
            stats.map(stat ⇒ f"${stat.consume}%s/${stat.countDaily}%.2f/${stat.costDaily}%.2f").mkString(",")
      }
      .write.text(args(1))
    spark.stop()
  }
}