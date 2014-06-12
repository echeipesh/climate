package climate

import climate.json._
import spray.json._

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
    for(row <- 0 until tileRows) {
      for(col <- 0 until tileCols) {
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

    def writeToPath(path: Path)(f: PrintWriter => Unit): Unit = {
      val fdos = fs.create(path)
      val out = new PrintWriter(fdos)
      f(out)
      out.close()
      fdos.close()
    }


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

    val splits = partitioner.splits

    val metadata = 
      TimeSeriesMetadata(builder.timeIds.size, splits, rasterExtent, builder.tileLayout, rasterType)

    writeToPath(new Path(outputDir, "metadata")) { out =>
      out.println(metadata.toJson.compactPrint)
    }

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

    val set = scala.collection.mutable.Set[Long]()
    val set2 = scala.collection.mutable.Set[(Int,Int)]()

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
            set += raster.data.lengthLong
            set2 += ((raster.cols, raster.rows))
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

    println(s"Result: ${set.toSeq}")
    println(s"Result: ${set2.toSeq}")
  }
}

import java.lang.System.currentTimeMillis

object Benchmark {
  def apply(times:Int)(body: => Unit):Long = {
    var i = 0
    var totalDuration = 0L
    while(i < times) {
      val start = currentTimeMillis()
      body
      totalDuration += currentTimeMillis() - start
      i += 1
    }

    totalDuration / (i+1)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
//    val outputDir = new Path("/home/rob/data/climate/test1")
    val outputDir = new Path("hdfs://localhost:9000/user/rob/test-raster")

    val pathHistorical = "/media/storage/monthly/tas/access1-0/historical/BCSD_0.5deg_tas_Amon_access1-0_historical_r1i1p1_195001-200512.nc"
    val pathFuture = "/media/storage/monthly/tas/access1-0/rcp45/BCSD_0.5deg_tas_Amon_access1-0_rcp45_r1i1p1_200601-210012.nc"

    val ingest = false

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
//      val ingest = new Ingester(Seq(pathHistorical, pathFuture), 1, 1)

      ingest(outputDir, config)

    } else {
      val sparkConf =
        new SparkConf()
          .setMaster("local[14]")
          .setAppName("Climate")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "geotrellis.spark.KryoRegistrator")

      val sparkContext = new SparkContext(sparkConf)

      // val rdd = TSRasterRDD(outputDir, sparkContext)

      // val sum: Raster = rdd.sum
      // val count = rdd.count
      // val mean = sum.mapDouble { d => d / count }
      //(-25.969268798828125,31.58228302001953)

      val start = TimeId(new org.joda.time.DateTime(2000,1,1,12,0,org.joda.time.DateTimeZone.UTC)).toInt
      val end = TimeId(new org.joda.time.DateTime(2050,1,1,12,0,org.joda.time.DateTimeZone.UTC)).toInt

      val tsr = TSRaster(outputDir, sparkContext).filter { tsr => 
        val i = tsr.timeId.toInt
        i >= start && i <= end
      }
      val duration = 
        Benchmark(10) {
          val sum = tsr.sum
          val count = tsr.timeCount
          val mean = sum.mapDouble(_/count)
          println(s"(Min,Max) = ${mean.findMinMaxDouble}, count = $count")
        }
      println(s"Took $duration milliseconds.")
    }
    println("Done.")
  }
}
