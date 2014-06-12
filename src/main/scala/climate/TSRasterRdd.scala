package climate

import climate.json._
import spray.json._

import geotrellis._
import geotrellis.raster.TileLayout
import geotrellis.spark._
import geotrellis.spark.formats._
import geotrellis.spark.utils.HdfsUtils

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import org.apache.hadoop.fs.Path

case class TimeSeriesRaster(timeId: TimeId, raster: Raster) {
  def toWritable() = 
    (TimeIdWritable(timeId.toInt), ArgWritable.fromRasterData(raster.data))
}

case class TSRasterRDD(val prev: RDD[TimeSeriesRaster])
  extends RDD[TimeSeriesRaster](prev) {

  def sum: Raster = {
    foldLeft(Raster.empty( { (tsr1, tsr2) =>
      val r = tsr1.raster.combineDouble(tsr2.raster) {
        (z1, z2) => z1 + z2 
      }
      TimeSeriesRaster(TimeId.EMPTY, r)
    }
    .raster
  }

  override def getPartitions: Array[Partition] = firstParent.partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent.iterator(split, context)

  def toWritable = 
    mapPartitions({ partition =>
      partition.map(_.toWritable)
    }, true)

  def mapTiles(f: TimeSeriesRaster => TimeSeriesRaster): TSRasterRDD =
    mapPartitions({ partition =>
      partition.map { tile =>
        f(tile)
      }
    }, true)
}

object TSRasterRDD {
  implicit def fromRdd(rdd:RDD[TimeSeriesRaster]): TSRasterRDD = 
    TSRasterRDD(rdd)
}

class TSRaster(metadata: TimeSeriesMetadata, rdds: Seq[TSRasterRDD]) {
  def sum: Raster =
    TileRaster(
      rdds.par.map(_.sum).toList,
//      rdds.map(_.sum),
      metadata.rasterExtent,
      metadata.tileLayout
    ).toArrayRaster

  def timeCount: Int = metadata.timeCount

  def filter(f: TimeSeriesRaster => Boolean): TSRaster =
    new TSRaster(metadata, rdds.map(rdd => TSRasterRDD.fromRdd(rdd.filter(f))))
}

object TSRaster {
  def apply(path: String, sc: SparkContext): TSRaster =
    apply(new Path(path), sc)

  def apply(path: Path, sc: SparkContext): TSRaster = {
    val metaPath = new Path(path, "metadata")

    val metadata: TimeSeriesMetadata =
      HdfsUtils.getLineScanner(metaPath, sc.hadoopConfiguration) match {
        case Some(in) =>
          try {
            in.mkString.parseJson.convertTo[TimeSeriesMetadata]
          }
          finally {
            in.close
          }
        case None =>
          sys.error(s"oops - couldn't find metadata here - ${metaPath.toUri.toString}")
      }


    val tileLayout = metadata.tileLayout
    val rdds =
      for(row <- 0 until tileLayout.tileRows;
          col <- 0 until tileLayout.tileCols) yield {
        val tileId = (row * tileLayout.tileCols) + col
        TSRasterHadoopRDD(
          new Path(path, f"tile-${tileId}%05d"),
          sc,
          metadata.partitioner,
          metadata.resolutionLayout.getRasterExtent(col, row),
          metadata.rasterType).toTSRasterRDD
      }

    new TSRaster(metadata, rdds)
  }
}
