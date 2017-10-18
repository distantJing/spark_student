package zhranklin

import org.apache.spark.sql.SparkSession

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object GoodFriends {

  case class Stat(consume: String, date: String, count: Int, totalCost: Double)

  val MILLIS_HALF_INTERVAL = 90000 // 1.5 min
  def findTimeWindow(index: Int, halfInterval: Int, records: Array[ConsumeRecord]): Seq[ConsumeRecord] = {
//    println(s"finding windows, this: ${records(index)}")
    val cur = records(index).timestamp
    val (min, max) = (cur - halfInterval, cur + halfInterval)
//    println(s"minmax: $min, $max")
    val range = ((index - 1 to 0 by -1)
      .takeWhile(records(_).timestamp >= min) ++
      (index + 1 until records.length)
        .takeWhile(records(_).timestamp < max))
      .map(records)
//    println(s"range is: $range")
    range
  }

  def computeDistance(curTime: Long, targets: Seq[ConsumeRecord]): List[(Int, Double)] = {
    val mhiSquare = MILLIS_HALF_INTERVAL.toDouble * MILLIS_HALF_INTERVAL
    val maxScore = mhiSquare / 10000 / 10000
    val result = targets.filter(_.timestamp != curTime)
      .map { r ⇒
        val t = r.timestamp - curTime
        r.student → Math.min(mhiSquare / t / t, maxScore)
      }
      .toList
//    println(s"curTime: $curTime, targets: $targets, result: $result")
    result
  }

  def main(args: Array[String]) {
    val outputFile = args(1)//"/Users/zhranklin/spark_student_good_friends.txt"
    implicit val spark: SparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._
    val totalMap: mutable.Map[Int, mutable.Map[Int, Double]] = TrieMap()
    DataPreProcessors.getConsumeStatRaw(args(0))
      .filter(r ⇒ notEmptyOrNull(r.place))
      //      .limit(1000)
      .groupByKey { record ⇒ (record.place, record.timestamp / MILLIS_PER_DAY) }
      .mapGroups {
        case (p_d@(place, days), recordsItr) ⇒
          val records = recordsItr.toList.sortBy(_.timestamp).toArray
//          println(s"p_d: $p_d, records: ${records.mkString(",")}")
          val scoreMap: List[(Int, List[(Int, Double)])] = records
            .zipWithIndex
            .map {
              case (record, index) ⇒
                val neighbours = findTimeWindow(index, MILLIS_HALF_INTERVAL, records)
                (record.student, computeDistance(record.timestamp, neighbours))
            }
            .groupBy(_._1) //根据student分组
            .mapValues { values ⇒ //同一学生, 对应多组分数
            val tuples = values.flatMap(_._2).toList
//            println(s"stu: ${values.head._1}, tuples: $tuples")
            val result = tuples //打散成(student, score)的list
              .groupBy(_._1) //根据student分组
              .mapValues(_.map(_._2).sum) //同一student分数求和
              .toList
//            println("rrr: " + result)
            result
          }
            .toList
          //得到student -> score的map
//          println(s"scoreMap of $p_d: $scoreMap")
          scoreMap
      }
//      .limit(100)
      .flatMap(identity)
      .flatMap {
        case (stu1, scores) ⇒
          scores.map {
            case (stu2, score) ⇒ (stu1, stu2, score)
          }
      }
      .groupBy("_1", "_2")
      .agg("_3" → "sum")
      .filter(row ⇒ row.getInt(0) < row.getInt(1)) //去掉重复以及与自身的关系, 与他人的关系正反都会算一遍
      .sort($"sum(_3)".desc)
      .repartition(1)//否则会输出一堆文件
      .write.csv(outputFile)
    spark.stop()
  }
}