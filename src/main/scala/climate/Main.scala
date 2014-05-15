package climate

import geotrellis._
import geotrellis.gdal._
import geotrellis.raster.TileLayout
import geotrellis.raster.RasterData
import geotrellis.spark.formats.ArgWritable
import geotrellis.spark.utils.HdfsUtils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.MapFile

import org.apache.commons.codec.binary.Base64

import java.io.PrintWriter
import java.nio.ByteBuffer

import scala.collection.mutable

import scala.collection.mutable

class IngestRasterBuilder(re: RasterExtent, tileCols: Int, tileRows: Int) {
  val tileLayout = TileLayout(re, tileCols, tileRows)
  val resolutionLayout = tileLayout.getResolutionLayout(re)

  val timeIds = mutable.ListBuffer[TimeId]()
  val rasterSeqs = Array.fill(tileCols * tileRows)(mutable.ListBuffer[TimeSeriesRaster]())

  def addRaster(time: TimeId, raster: Raster): Unit = {
    timeIds += time
    for(col <- 0 until tileCols) {
      for(row <- 0 until tileRows) {
        rasterSeqs(row*tileCols + col) += 
          TimeSeriesRaster(time, CroppedRaster(raster, resolutionLayout.getExtent(col, row)))
      }
    }
  }
}

class Ingester(val paths: Seq[String], tileCols: Int, tileRows: Int) {
  // Gather metadata
  val (rasterExtent, rasterType) = {
    val rasterDataSet = Gdal.open(paths(0))
    try {
      (rasterDataSet.rasterExtent, rasterDataSet.band(1).toRasterData.getType)
    } finally {
      rasterDataSet.close
    }
  }

  if((rasterExtent.cols / tileCols).toDouble != (rasterExtent.cols / tileCols.toDouble)) {
    sys.error(s"Tile columns ($tileCols) does not evenly divide raster extent $rasterExtent")
  }

  if((rasterExtent.rows / tileRows).toDouble != (rasterExtent.rows / tileRows.toDouble)) {
    sys.error(s"Tile rows ($tileRows) does not evenly divide raster extent $rasterExtent")
  }

  def apply(outputDir: Path, config: Configuration): Unit = {
    val fs = outputDir.getFileSystem(config)
    val blockSize = fs.getDefaultBlockSize()

    println(s"Deleting and creating output path: $outputDir")
    fs.delete(outputDir, true)
    fs.mkdirs(outputDir)

    var maxTime = Int.MinValue
    var minTime = Int.MaxValue

    val builder = new IngestRasterBuilder(rasterExtent, tileCols, tileRows)

    for(path <- paths) {
      val rasterDataSet = Gdal.open(path)
      val bandCount = rasterDataSet.bandCount

      if(rasterExtent != rasterDataSet.rasterExtent) {
        sys.error(s"Raster Extents don't match: $rasterExtent and ${rasterDataSet.rasterExtent}")
      }

      for(i <- 1 to bandCount) {
        val band = rasterDataSet.band(i)
        val meta =
          band
            .metadata
            .map(_.split("="))
            .map(l => { (l(0), l(1)) })
            .toMap

        val timeInt = meta("NETCDF_DIM_Time").toDouble.toInt
        maxTime = math.max(maxTime, timeInt)
        minTime = math.min(minTime, timeInt)
        val time = TimeId(timeInt)
        val raster = Raster(band.toRasterData, rasterExtent)
        println(s"Adding ${time.date}")
        builder.addRaster(time, raster)
      }
    }

    val partitioner =
      TimeSeriesPartitioner(
        builder.timeIds,
        rasterExtent,
        rasterType,
        blockSize
      )

    // Write splits
    val splits = partitioner.splits
    val splitFile = new Path(outputDir, "splits")
    println("writing splits to " + splitFile)
    val fdos = fs.create(splitFile)
    val out = new PrintWriter(fdos)
    splits.foreach {
      split => out.println(new String(Base64.encodeBase64(ByteBuffer.allocate(4).putInt(split).array())))
    }
    out.close()
    fdos.close()

    println(s"Splits: ${splits.toSeq}")
    println(s"NUM PARTITIONS: ${partitioner.numPartitions}")

    // open as many writers as number of partitions
    def openWriters(rasterPath: Path, num: Int) = {
      val writers = new Array[MapFile.Writer](num)
      for (i <- 0 until num) {
        val mapFilePath = new Path(rasterPath, f"part-${i}%05d")

        writers(i) = new MapFile.Writer(config, fs, mapFilePath.toUri.toString,
          classOf[TimeIdWritable], classOf[ArgWritable], SequenceFile.CompressionType.RECORD)

      }
      writers
    }

    for(tileId <- 0 until builder.rasterSeqs.length) {
      val rasterPath = new Path(outputDir, f"tile-${tileId}%05d")
      fs.mkdirs(rasterPath)

      val timeSeriesRasters = builder.rasterSeqs(tileId)

      val key = new TimeIdWritable()

      val writers = openWriters(rasterPath, partitioner.numPartitions)

      try {
        timeSeriesRasters.foreach {
          case TimeSeriesRaster(timeId, raster) => {
            key.set(timeId.timeId)
            writers(partitioner.getPartition(timeId.timeId)).append(key, ArgWritable.fromRasterData(raster.data))
            println(s"Saved tileId=${timeId},partition=${partitioner.getPartition(timeId.timeId)}")
          }
        }
      }
      finally {
        writers.foreach(_.close)
      }
      println(s"Done saving ${timeSeriesRasters.length} tiles")
    }
  }
}


object Main {
  def main(args: Array[String]): Unit = {
//    val outputDir = new Path("/home/rob/data/climate/test1")
    val outputDir = new Path("hdfs://localhost:9000/user/rob/test-raster")

    val pathHistorical = "/media/storage/monthly/tas/access1-0/historical/BCSD_0.5deg_tas_Amon_access1-0_historical_r1i1p1_195001-200512.nc"
    val pathFuture = "/media/storage/monthly/tas/access1-0/rcp45/BCSD_0.5deg_tas_Amon_access1-0_rcp45_r1i1p1_200601-210012.nc"

    val ingest = true

    // Hadoop config
    val config = {
      Configuration.addDefaultResource("core-site.xml")
      Configuration.addDefaultResource("mapred-site.xml")
      Configuration.addDefaultResource("hdfs-site.xml")
      new Configuration
    }
    config.set("io.map.index.interval", "1")

    if(ingest) {
      val fs = outputDir.getFileSystem(config)
      val blockSize = fs.getDefaultBlockSize()

      println(s"Deleting and creating output path: $outputDir")
      fs.delete(outputDir, true)
      fs.mkdirs(outputDir)

      val ingest = new Ingester(Seq(pathHistorical, pathFuture), 4, 2)

      ingest(outputDir, config)

    } else {
      val sparkConf =
        new SparkConf()
          .setMaster("local[6]")
          .setAppName("Climate")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "geotrellis.spark.KryoRegistrator")

      val sparkContext = new SparkContext(sparkConf)

      val splitFile = new Path(outputDir, "splits")

//      val rdd = TSRasterRDD(
//      val rd: RasterData = rdd.sum
    }
    println("Done.")
  }
}
