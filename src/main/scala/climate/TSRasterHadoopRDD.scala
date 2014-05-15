package climate

import geotrellis._
import geotrellis.spark.formats.ArgWritable

import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.SparkContext

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

class TimeSeriesRasterPartitioner extends org.apache.spark.Partitioner {
  def getPartition(key: Any): Int = ???
  def numPartitions: Int = ???
}

class TSRasterHadoopRDD (sc: SparkContext, conf: Configuration, tsPartitioner: TimeSeriesRasterPartitioner, rasterType: RasterType, rasterExtent: RasterExtent)
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
   * 	e.g., file:///tmp/timeseriesraster/10/
   *   
   * sc - the spark context
   */
  def apply(raster: String, sc: SparkContext): TSRasterHadoopRDD =
    apply(new Path(raster), sc)

  def apply(raster: Path, sc: SparkContext): TSRasterHadoopRDD = {
    val job = new Job(sc.hadoopConfiguration)
    val globbedPath = new Path(raster.toUri().toString() + SeqFileGlob)
    FileInputFormat.addInputPath(job, globbedPath)
    val updatedConf = job.getConfiguration

    // val splitFile = new Path(outputDir, "splits")
    // HdfsUtils.getLineScanner(splitFile, config) match {
    //   case Some(in) =>
    //     try {
    //       val splits = mutable.ListBuffer[Int]()
    //       for (line <- in) {
    //         splits += ByteBuffer.wrap(Base64.decodeBase64(line.getBytes)).getInt
    //       }
    //       splits.toArray
    //     }
    //     finally {
    //       in.close
    //     }
    //   case None =>
    //     Array[Int]()
    // }

    val partitioner = ???
    val (rasterType, rasterExtent) = (???, ???)

    new TSRasterHadoopRDD(sc, updatedConf, partitioner, rasterType, rasterExtent)
  }
}
