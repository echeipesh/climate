package climate

import org.joda.time._

case class TimeId(timeId: Int) {
  lazy val date: DateTime = 
    TimeId.startDate.plusDays(timeId)

  def toInt(): Int = timeId
}

object TimeId {
  val startDate = new DateTime(1950,1,1,12,0,DateTimeZone.UTC)

  val EMPTY = TimeId(0)
}

case class TimeRange(startTime: TimeId, endTime: TimeId) { 
  lazy val length: Int = (endTime.timeId - startTime.timeId)
}
