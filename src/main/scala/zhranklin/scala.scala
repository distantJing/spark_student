import java.util.Date

/**
  * Created by Zhranklin zhangwu@corp.netease.com at 2017/9/22
  */
package object zhranklin {
  implicit def dateToLong(date: Date): Long = date.getTime
  implicit def longToDate(long: Long): Date = new Date(long)
  case class ConsumeRecord(student: Int, consume: String, timestamp: Long, cost: Int, place: String)
  case class DormRecord(student: Int, timestamp: Long, action: Int)
  case class LibraryRecord(student: Int, place: String, timestamp: Long)
  case class ScoreRecord(student: Int, college: Int, score: Int)
  case class CollegeInfoRecord(college: Int, count: Int)
  case class FpgRecord(timeOut: Int, timeIn: Int, libraryCount: Int, scorePercent: Double)
  val cardFile = "C:/数据分析/精准资助/train/card_train.txt"

  def notEmptyOrNull(str: String) = str != null && str.nonEmpty
  val MILLIS_PER_DAY = 86400000


}
