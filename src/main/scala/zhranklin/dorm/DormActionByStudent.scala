package zhranklin

import java.util.Date
import java.io._

import org.apache.spark.sql.SparkSession

// 原始数据：13126,"2014/01/21 04:53:55","0"
// 每个人平均每天初入门次数，平均出门归寝时间，结果如下：
// 10,0,1.52,17:44  编号为10的同学平均每天入门1.52次，平均归寝时间17：44
// 10,1,1.55,12:42
object DormActionByStudent {
  case class Stat(student: Int, action: Int, count: Int, time: Int) // time分钟数
  case class StatRaw(student: Int, action: Int, avgCountPerDay: Double, avgTime: String)
  val f = new FileOutputStream("C:/IdeaProjects/untitled2/dormActionByStudent.txt")
  def main(args: Array[String]): Unit = {
    val cardFile = "C:/数据分析/精准资助/train/dorm_train.txt"
    implicit val spark: SparkSession = SparkSession.builder().appName("Simple Application").getOrCreate()
    import spark.implicits._
    // 13126,2014/01/21 04:53:55,0
    DataPreProcessors.getDormStatRaw(cardFile)
      .groupByKey(record ⇒ (record.student, record.timestamp / 86400000, record.action))
      .mapGroups {
        case ((student, _, action), recordItr) ⇒
          val records = recordItr.toList
          var count = 0; var time = 0
          action match {
            case 0 ⇒ {
              count = records.size
              time = records.map(record ⇒ record.timestamp.getHours*60+record.timestamp.getMinutes).max
            }

            case 1 ⇒ {
              count = records.size
              time = records.map(record ⇒ record.timestamp.getHours*60+record.timestamp.getMinutes).min
            }
          }
          Stat(student, action, count, time)
      }
      .groupByKey(record ⇒ (record.student, record.action))
      .mapGroups{
        case ((student, action), recordItr) ⇒
          val records = recordItr.toList
          var (totalCount, totalTime) = records.foldLeft((0, 0)) {
            case ((totalCount0, totalTime0), record) ⇒ (totalCount0 + record.count, totalTime0 + record.time)
          }
          var avgMinite = totalTime/records.size.toInt
          var avgTime = (avgMinite/60).toString + ":" + (avgMinite%60).toString
          (StatRaw(student, action, totalCount*1.0/records.size, avgTime), (student, action))
      }
      .sort("_1")
      .map{tp ⇒ import tp._1._; f"$student,$action,$avgCountPerDay%.2f,$avgTime"}
      .map(data ⇒ (data.toString+"\r\n").getBytes())
      .foreach(f.write(_))
    f.close()
    spark.stop()
  }
}
