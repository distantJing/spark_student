package zhranklin

import java.io.FileOutputStream
import java.util.Date

import org.apache.spark.sql.SparkSession

// 8564,"6","2013/09/07 16:53:12"

object LibraryCountByStudent {
  case class Stat(student: Int, count: Int, minTime: Long, maxTime: Long)
  case class StatRaw(student: Int, count:Int, countDaily: Double)
  val f = new FileOutputStream("C:/IdeaProjects/untitled2/LibraryCountByStudent.txt")
  def main(args: Array[String]) {
    val cardFile = "C:/数据分析/精准资助/train/library_train.txt"
    implicit val spark: SparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._
    DataPreProcessors.getLibraryStatRaw(cardFile)
      .groupByKey(record ⇒ (record.student))
      .mapGroups {
        case (student, recordItr) ⇒
          val records = recordItr.toList
          val count = records.size
          val minTime = new Date(records.map(_.timestamp).min)
          val maxTime = new Date(records.map(_.timestamp).max)
          (Stat(student, count, minTime, maxTime), (count, student))
      }
      .sort($"_2".desc)
      .map{tp ⇒ import tp._1._; s"$student,$count,$minTime,$maxTime"}
      .map(data ⇒ (data.toString+"\r\n").getBytes())
      .foreach(f.write(_))
    f.close()
    spark.stop()
  }
}
