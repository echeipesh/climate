package climate

import geotrellis.raster._

import scala.collection.mutable

class TimeSeriesPartitioner(val splits: Array[Int])  extends org.apache.spark.Partitioner {

  // @transient
  // private var splits = new Array[TimeIdWritable](0)

  override def getPartition(key: Any) = key match {
    case i: Int =>
      val index = java.util.Arrays.binarySearch(splits, i)
      if (index < 0)
        (index + 1) * -1
      else
        index
    case _ => sys.error(s"Invalid key: ${key.getClass.getName}")
  }

  // get min,max timeId in a given partition
  def range(partition: Int): (Int, Int) = {
    val min = if (partition == 0) Int.MinValue else splits(partition - 1) + 1
    val max = if (partition == splits.length) Int.MaxValue else splits(partition)
    (min, max)
  }

  override def numPartitions = splits.length + 1

  override def toString = "TimeSeriesPartitioner split points: " + {
    if (splits.isEmpty) "Empty" else splits.zipWithIndex.mkString
  }
}

object TimeSeriesPartitioner {
  def apply(
    timeIds: Traversable[TimeId],
    cols: Int,
    rows: Int,
    cellType: CellType,
    blockSize: Long
  ): TimeSeriesPartitioner = {
    val tilesPerBlock = (blockSize / (cols * rows * cellType.bytes)).toInt
    val splits = mutable.ListBuffer[Int]()
    var count = 1
    for(time <- timeIds) {
      if(count == tilesPerBlock) {
        splits += time.timeId
        count = 1
      } else {
        count += 1
      }
    }
    new TimeSeriesPartitioner(splits.toArray)
  }
}
