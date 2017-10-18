package zhranklin

import java.util.Date
import java.io._

import org.apache.spark.sql.SparkSession
// 原始数据：13126,"2014/01/21 04:53:55","0"
// 统计每个时间段出入门的总人数，perMin, perHour， 结果如下：
// 2014/03/20 22:38, 0, 10   该时间段共10人进门
// 2014/03/20 22:39, 0, 4
object DormActionByTime {
  case class Stat(date: String, action: Int, count: Int)
  val f = new FileOutputStream("C:/IdeaProjects/untitled2/dormActionByTimePerHour.txt")
  def main(args: Array[String]): Unit ={
    val cardFile = "C:/数据分析/精准资助/train/dorm_train.txt"
    implicit val spark: SparkSession = SparkSession.builder().appName("Simple Application").getOrCreate()
    import spark.implicits._
    // 13126,2014/01/21 04:53:55,0
    DataPreProcessors.getDormStatRaw(cardFile)
      .groupByKey(record ⇒ (record.timestamp / 3600000, record.action))
      .mapGroups {
        case ((_, action), recordItr) ⇒
          val records = recordItr.toList
          var d: Date = records.head.timestamp
          var dateStr = f"$d%tY/$d%tm/$d%td $d%tH:$d%tM"
          dateStr = f"$d%tY/$d%tm/$d%td $d%tH"
          (Stat(dateStr, action, records.size), (action, dateStr))
      }
      .sort("_1")
      .map { tp ⇒ import tp._1._; s"$date, $action, $count" }
      .map(data ⇒ (data.toString+"\r\n").getBytes())
     // .foreach(println(_))
      .foreach(f.write(_))
    spark.stop()
  }
}
