package  zhranklin
import java.io.FileOutputStream

import org.apache.spark.sql.SparkSession

// 学生id,学院编号,成绩排名
// 0,9,1
// 结果：学院，学号，成绩排名，成绩排名百分比
object ScoreByStudent {
  val f = new FileOutputStream("C:/IdeaProjects/untitled2/ScoreByStudent.txt")
  def main(args: Array[String]) {
    val cardFile = "C:/数据分析/精准资助/train/score_train.txt"
    implicit val spark: SparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._

    var temp = DataPreProcessors.getScoreStatRaw(cardFile)  // .sort($"college", $"score")
    val collegeInfo = temp
      .groupByKey(record ⇒ record.college)
      .mapGroups{
        case (college, recordItr) ⇒
          val records = recordItr.toList
          val count = records.map(_.score).max
          (college, count)
      }
      .map(data ⇒ CollegeInfoRecord(data._1, data._2))

    temp.join(collegeInfo, "college")     // college, student, score, count
      .sort("college", "student")
      .map{
          case data ⇒
            val scorePercent = data(2).toString.toInt * 1.0 / data(3).toString.toInt
            f"${data(0)},${data(1)},${data(2)},$scorePercent%.2f"
        }
      .map(data ⇒ (data.toString+"\r\n").getBytes())
      .foreach(f.write(_))

    f.close()
    spark.stop()
  }
}
