package climate

import geotrellis._

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

  override def numPartitions = splits.length + 1

  override def toString = "TimeSeriesPartitioner split points: " + {
    if (splits.isEmpty) "Empty" else splits.zipWithIndex.mkString
  }
}

object TimeSeriesPartitioner {
  def apply(
    timeIds: Traversable[TimeId],
    rasterExtent: RasterExtent,
    rasterType: RasterType,
    blockSize: Long
  ): TimeSeriesPartitioner = {
    val rastersPerBlock = (blockSize / (rasterExtent.cols * rasterExtent.rows * rasterType.bytes)).toInt
    val splits = mutable.ListBuffer[Int]()
    var count = 1
    for(time <- timeIds) {
      if(count == rastersPerBlock) {
        splits += time.timeId
        count = 1
      } else {
        count += 1
      }
    }
    new TimeSeriesPartitioner(splits.toArray)
  }
}
