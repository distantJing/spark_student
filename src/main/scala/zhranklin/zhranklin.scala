import java.util.Date

/**
 * Created by Zhranklin zhangwu@corp.netease.com at 2017/9/22
 */
package object zhranklin {
  implicit def dateToLong(date: Date): Long = date.getTime
  implicit def longToDate(long: Long): Date = new Date(long)
  val MILLIS_PER_DAY = 86400000
  def notEmptyOrNull(str: String) = str != null && str.nonEmpty
  case class ConsumeRecord(student: Int, consume: String, timestamp: Long, cost: Int, place: String)
}
