package climate

import climate.json._
import spray.json._

import geotrellis.raster._
import geotrellis.raster.op.local._
import geotrellis.spark._
import geotrellis.spark.formats._
import geotrellis.spark.utils.HdfsUtils

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util._

import org.apache.hadoop.fs.Path

case class TimeSeriesRaster(timeId: TimeId, tile: Tile) {
  def toWritable() = 
    (TimeIdWritable(timeId.toInt), ArgWritable.fromTile(tile))
}

case class TSRasterRDD(val prev: RDD[TimeSeriesRaster])
  extends RDD[TimeSeriesRaster](prev) {

  def sum(cols: Int, rows: Int): Tile = {
    val zeroValue = TimeSeriesRaster(TimeId.EMPTY, DoubleConstantTile(0.0, cols, rows))
    val op = { (tsr1: TimeSeriesRaster, tsr2: TimeSeriesRaster) =>
//      println(s"         ${tsr1.tile.cols}x${tsr2.tile.rows}")
      TimeSeriesRaster(
        TimeId.EMPTY,
  //      Benchmark("  Tile Combine") {
          tsr1.tile.combineDouble(tsr2.tile) {
            (z1, z2) => z1 + z2
          }
//        }
      )
    }
    val rdd =
      Benchmark("COLLECT RASTERS") {
        this.collect
      }
    println(s"  Size: ${rdd.size}")
    Benchmark("TSRasterRDD Sum") {
      rdd.fold(zeroValue)(op)
    }.tile
  }

  // def sum(cols: Int, rows: Int): Tile = {
  //   val initial = DoubleConstantTile(0.0, cols, rows)
  //   Benchmark("TSRasterRDD Sum2") {
  //     mapPartitions { partition =>
  //       Seq(partition.map(_.tile).toSeq.localAdd).iterator
  //     }
  //     .fold(initial) { _ + _ }
  //   }
  // }


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
