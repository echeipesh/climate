package climate

import geotrellis._
import geotrellis.raster.TileLayout
import geotrellis.spark._
import geotrellis.spark.formats._

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
    reduce { (tsr1, tsr2) => 
      val rd = tsr1.raster.combineDouble(tsr2.raster) {
        (z1, z2) => z1 + z2 
      }
      TimeSeriesRaster(TimeId.EMPTY, rd)
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

  def apply(raster: String, sc: SparkContext): TSRasterRDD =
    apply(new Path(raster), sc)

  def apply(raster: Path, sc: SparkContext): TSRasterRDD =
    TSRasterHadoopRDD(raster, sc).toTSRasterRDD
}

class TSRaster(rasterExtent: RasterExtent, tileLayout: TileLayout, rdds: Seq[TSRasterRDD]) {
  def mean: Raster = ???

  def toWritable(tr: Tile): WritableTile =
    (TileIdWritable(tr.id), ArgWritable.fromRasterData(tr.raster.data))
}

object TSRaster {
  def apply(path: String, sc: SparkContext): TSRaster = {
    val meta: (RasterExtent, TileLayout) = ??? // read metadata from path
    val (re, tileLayout) = meta
    val rdds =
      (0 until tileLayout.tileCols * tileLayout.tileRows).map { tileId =>
        TSRasterHadoopRDD(s"$path/$tileId", sc).toTSRasterRDD
      }

    new TSRaster(re, tileLayout, rdds)
  }
}
