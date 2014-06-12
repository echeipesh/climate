package climate

import climate.json._
import spray.json._

import geotrellis._
import geotrellis.spark.formats.ArgWritable
import geotrellis.spark.utils.HdfsUtils

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.SparkContext

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

class TSRasterHadoopRDD (sc: SparkContext, conf: Configuration, tsPartitioner: TimeSeriesPartitioner, rasterType: RasterType, rasterExtent: RasterExtent)
  extends NewHadoopRDD[TimeIdWritable, ArgWritable](
    sc,
    classOf[SequenceFileInputFormat[TimeIdWritable, ArgWritable]],
    classOf[TimeIdWritable],
    classOf[ArgWritable],
    conf) {

  /*
   * Overriding the partitioner with a TileIdPartitioner 
   */
  override val partitioner = Some(tsPartitioner)

//  @transient val pyramidPath = raster.getParent()
//  val meta = PyramidMetadata(pyramidPath, conf)

  def toTSRasterRDD(): TSRasterRDD = 
    mapPartitions { partition =>
      partition.map { case (timeIdWritable, argWritable) =>
        TimeSeriesRaster(TimeId(timeIdWritable.get), 
          Raster(argWritable.toRasterData(rasterType, rasterExtent.cols, rasterExtent.rows), rasterExtent))
      }
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
    rasterExtent: RasterExtent, 
    rasterType: RasterType
  ): TSRasterHadoopRDD = {
    val job = new Job(sc.hadoopConfiguration)
    val globbedPath = new Path(rasterPath.toUri().toString() + SeqFileGlob)
    FileInputFormat.addInputPath(job, globbedPath)
    val updatedConf = job.getConfiguration

    new TSRasterHadoopRDD(sc, updatedConf, partitioner, rasterType, rasterExtent)
  }
}
