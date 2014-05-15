package climate

import org.apache.hadoop.io.IntWritable

import java.io.ObjectInputStream
import java.io.ObjectOutputStream

class TimeIdWritable extends IntWritable with Serializable {
  override def equals(that: Any): Boolean =
    that match {
      case other: TimeIdWritable => other.get == this.get
      case _ => false
    }

  override def hashCode = get.hashCode
  
  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    out.writeLong(get)
  }

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    set(in.readInt)
  }

  def toTSTileId(): TimeId = 
    TimeId(get)
}

object TimeIdWritable {
  def apply(value: Int): TimeIdWritable = {
    val tw = new TimeIdWritable
    tw.set(value)
    tw
  }
  def apply(tw: TimeIdWritable): TimeIdWritable = {
    TimeIdWritable(tw.get)
  }
}
