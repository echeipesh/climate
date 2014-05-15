package climate

import geotrellis._

import scala.collection.mutable

class TimeSeriesPartitioner(val splits: Array[Int], val rastersPerBlock: Int)  extends org.apache.spark.Partitioner {

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
    new TimeSeriesPartitioner(splits.toArray, rastersPerBlock)
  }
}

//   final val SplitFile = "splits"

//   /* construct a partitioner from the splits file, if one exists */
//   def apply(raster: Path, conf: Configuration): TileIdPartitioner = {
//     val tp = new TileIdPartitioner
//     tp.splits = readSplits(raster, conf)
//     tp
//   }

//   /* construct a partitioner from the splits file, if one exists */
//   def apply(raster: String, conf: Configuration): TileIdPartitioner = {
//     apply(new Path(raster), conf)
//   }

//   /* construct a partitioner from a split generator */
//   def apply(splitGenerator: SplitGenerator, raster: Path, conf: Configuration): TileIdPartitioner = {
//     writeSplits(splitGenerator, raster, conf)
//     apply(raster, conf)
//   }

//   private def readSplits(raster: Path, conf: Configuration): Array[TileIdWritable] = {
//     val splitFile = new Path(raster, SplitFile)
//     HdfsUtils.getLineScanner(splitFile, conf) match {
//       case Some(in) =>
//         try {
//           val splits = new ListBuffer[TileIdWritable]
//           for (line <- in) {
//             splits +=
//               TileIdWritable(ByteBuffer.wrap(Base64.decodeBase64(line.getBytes)).getLong)
//           }
//           splits.toArray
//         }
//         finally {
//           in.close
//         }
//       case None =>
//         Array[TileIdWritable]()
//     }
//   }

//   private def writeSplits(splitGenerator: SplitGenerator, raster: Path, conf: Configuration): Int = {
//     val splits = splitGenerator.getSplits
//     val splitFile = new Path(raster, SplitFile)
//     //println("writing splits to " + splitFile)
//     val fs = splitFile.getFileSystem(conf)
//     val fdos = fs.create(splitFile)
//     val out = new PrintWriter(fdos)
//     splits.foreach {
//       split => out.println(new String(Base64.encodeBase64(ByteBuffer.allocate(8).putLong(split).array())))
//     }
//     out.close()
//     fdos.close()

//     splits.length
//   }

//   private def printSplits(raster: Path, conf: Configuration): Unit = {
//     val zoom = raster.getName().toInt
//     val splits = readSplits(raster, conf)
//     splits.zipWithIndex.foreach {
//       case (tileId, index) => {
//         val (tx, ty) = TmsTiling.tileXY(tileId.get, zoom)
//         println(s"Split #${index}: tileId=${tileId.get}, tx=${tx}, ty=${ty}")
//       }
//     }

//     // print the increments unless there were no splits
//     if (!splits.isEmpty) {
//       val meta = PyramidMetadata(raster.getParent(), conf)
//       val tileExtent = meta.metadataForBaseZoom.tileExtent
//       val (tileSize, rasterType) = (meta.tileSize, meta.rasterType)

//       val inc = RasterSplitGenerator.computeIncrement(tileExtent,
//         TmsTiling.tileSizeBytes(tileSize, rasterType),
//         HdfsUtils.defaultBlockSize(raster, conf))
//       println(s"(xInc,yInc) = ${inc}")
//     }
//   }

//   def main(args: RasterArgs) {
//     val input = new Path(args.inputraster)
//     val conf = SparkUtils.createHadoopConfiguration
//     printSplits(input, conf)
//   }
//}
