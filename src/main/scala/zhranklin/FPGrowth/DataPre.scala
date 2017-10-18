package zhranklin

import java.io.FileOutputStream
import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}

object DataPre {
  val filePath = "C:/IdeaProjects/untitled2/"
  val dormFilename = "dormActionByStudent.txt"
  val libraryFilename = "LibraryCountByStudent.txt"
  val scoreFilename = "ScoreByStudent.txt"
  case class DormData(student: Int, timeIn: Int, timeOut: Int)
  case class LibraryData(student: Int, count: Int)
  case class ScoreData(student: Int, scorePercent: Double)
  var f = new FileOutputStream("C:/IdeaProjects/untitled2/student_source_Origin.txt")
  val f1 = new FileOutputStream("C:/IdeaProjects/untitled2/student_source_abstract.txt")
  def getOriginData()(implicit spark: SparkSession): Unit  = {
    //val f = new FileOutputStream("C:/IdeaProjects/untitled2/student_source_Origin.txt")
    import spark.implicits._
    val dorm = spark.read.textFile(filePath+dormFilename).cache()
      .map(_.split(","))
      .map{
        data ⇒
//          val timeStr = data._4.split(":")
//          val time = timeStr(0).toInt * 60 + timeStr(1).toInt
//          DormData(data._1.toInt, data._2.toInt, time.toInt) // student, action, time
          val timeStr = data(3).split(":")
          val time = timeStr(0).toInt * 60 + timeStr(1).toInt
          DormData(data(0).toInt, data(1).toInt, time.toInt) // student, action, time
      }
      .groupByKey(record ⇒ (record.student))
      .mapGroups{
        case (student, recordItr) ⇒
          val records = recordItr.toList
          val timeIn = records.map(_.timeOut).max
          val timeOut = records.map(_.timeOut).min
          DormData(student, timeIn, timeOut)
      }
    val library = spark.read.textFile(filePath + libraryFilename).cache()
      .map(_.split(","))
      .map(data ⇒ LibraryData(data(0).toInt, data(1).toInt))
    val score = spark.read.textFile(filePath + scoreFilename).cache()
      .map(_.split(","))
      .map(data ⇒ ScoreData(data(1).toInt, data(3).toDouble))

    // (student, scorePercent, count, timeIn, timeOut)
    val x = score.join(library, "student").join(dorm, "student")
      .sort("student")
        .map(data ⇒ f"${data(0)},${data(1)},${data(2)},${data(3)},${data(4)}")
      .map(data ⇒ (data.toString+"\r\n").getBytes())
      .foreach(f.write(_))

    f.close()
  }

  def getFpgData(filename: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    // (student, scorePercent, count, timeIn, timeOut)
    spark.read.textFile(filename).cache()
      .map(_.split(","))
      .map{
        case data ⇒
          val scoreLevel = (data(1).toDouble * 100).toInt / 20
          val libraryLevel = data(2).toInt / 50
          val inLevel = data(3).toInt / 120
          val outLevel = data(4).toInt / 120
          f"score${scoreLevel} library${libraryLevel} inLevel${inLevel} outLevel${outLevel}"
      }
      .map(data ⇒ (data.toString+"\r\n").getBytes())
      .foreach(f1.write(_))
    f1.close()
    spark.stop()
  }

  def main(args: Array[String]) {
    implicit val spark: SparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()
    getOriginData()
    getFpgData("C:\\IdeaProjects\\untitled2/student_source_Origin.txt")
  }
}