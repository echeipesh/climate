package climate

import climate.json._
import spray.json._

import geotrellis.raster._
import geotrellis.spark.formats.ArgWritable
import geotrellis.spark.utils.HdfsUtils

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.SparkContext
import org.apache.spark.Partition

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

class TSRasterHadoopRDD (sc: SparkContext, conf: Configuration, tsPartitioner: TimeSeriesPartitioner, cellType: CellType, cols: Int, rows: Int)
  extends FilteredHadoopRDD[TimeIdWritable, ArgWritable](
    sc,
    classOf[SequenceFileInputFormat[TimeIdWritable, ArgWritable]],
    classOf[TimeIdWritable],
    classOf[ArgWritable],
    conf) {

  /*
   * Overriding the partitioner with a TileIdPartitioner 
   */
  override val partitioner = Some(tsPartitioner)

  def toTSRasterRDD(): TSRasterRDD = 
    mapPartitions { partition =>
      partition.map { case (timeIdWritable, argWritable) =>
        Benchmark("To TimeSeriesRaster") { 
          TimeSeriesRaster(
            TimeId(timeIdWritable.get),
            argWritable.toTile(cellType, cols, rows)
          )
        }
      }
    }

  val start = TimeId(new org.joda.time.DateTime(2049,1,1,12,0,org.joda.time.DateTimeZone.UTC)).toInt
  val end = TimeId(new org.joda.time.DateTime(2050,1,1,12,0,org.joda.time.DateTimeZone.UTC)).toInt


  def includeKey(key: TimeIdWritable): Boolean = { 
    start <= key.get && key.get <= end
//    true 
  }

  def includePartition(p: Partition): Boolean = { 
    val (partMin, partMax) = partitioner.get.range(p.index)
//    println(s"$partMin,$partMax  $start,$end ${partMin <= end && start <= partMax}")
    partMin <= end && start <= partMax
  }

}

object TSRasterHadoopRDD {

  final val SeqFileGlob = "/*[0-9]*/data"

  /* raster - fully qualified path to the time series data
   * 	e.g., file:///tmp/timeseriesraster/
   *   
   * sc - the spark context
   */
  def apply(
    rasterPath: Path, 
    sc: SparkContext, 
    partitioner: TimeSeriesPartitioner, 
    cellType: CellType,
    cols: Int,
    rows: Int
  ): TSRasterHadoopRDD = {
    val job = new Job(sc.hadoopConfiguration)
    val globbedPath = new Path(rasterPath.toUri().toString() + SeqFileGlob)
    FileInputFormat.addInputPath(job, globbedPath)
    val updatedConf = job.getConfiguration

    new TSRasterHadoopRDD(sc, updatedConf, partitioner, cellType, rows, cols)
  }
}
