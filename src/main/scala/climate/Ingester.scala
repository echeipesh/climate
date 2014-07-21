package climate

import climate.json._
import spray.json._

import geotrellis.raster._
import geotrellis.feature.Extent
import geotrellis.gdal._
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

class IngestRasterBuilder(extent: Extent, val tileLayout: TileLayout) {
  val tileExtents = TileExtents(extent, tileLayout)

  val timeIds = mutable.ListBuffer[TimeId]()
  val rasterSeqs = 
    Array.fill(tileLayout.tileCols * tileLayout.tileRows)(mutable.ListBuffer[TimeSeriesRaster]())

  def addRaster(time: TimeId, tile: Tile): Unit = {
    timeIds += time
    for(row <- 0 until tileLayout.tileRows) {
      for(col <- 0 until tileLayout.tileCols) {
        val tsr = TimeSeriesRaster(time, CroppedTile(tile, extent, tileExtents(col, row)))
        rasterSeqs(row * tileLayout.tileCols + col)+= tsr
      }
    }
  }
}

class Ingester(val paths: Seq[String], tileCols: Int, tileRows: Int) {
  // Gather metadata
  val (rasterExtent, cellType) = {
    val rasterDataSet = Gdal.open(paths(0))
    try {
      (rasterDataSet.rasterExtent, rasterDataSet.band(1).toTile.cellType)
    } finally {
      rasterDataSet.close
    }
  }

  val pixelCols = rasterExtent.cols / tileCols
  if(pixelCols.toDouble != (rasterExtent.cols / tileCols.toDouble)) {
    sys.error(s"Tile columns ($tileCols) does not evenly divide raster extent $rasterExtent")
  }

  val pixelRows = rasterExtent.rows / tileRows
  if(pixelRows.toDouble != (rasterExtent.rows / tileRows.toDouble)) {
    sys.error(s"Tile rows ($tileRows) does not evenly divide raster extent $rasterExtent")
  }

  val tileLayout = TileLayout(tileCols, tileRows, pixelCols, pixelRows)

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

    val builder = new IngestRasterBuilder(rasterExtent.extent, tileLayout)

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
        val tile = band.toTile
        println(s"Adding ${time.date}")
        builder.addRaster(time, tile)
      }
    }

    val partitioner =
      TimeSeriesPartitioner(
        builder.timeIds,
        rasterExtent.cols,
        rasterExtent.rows,
        cellType,
        blockSize
      )

    val splits = partitioner.splits

    val metadata = 
      TimeSeriesMetadata(builder.timeIds.size, splits, rasterExtent.extent, builder.tileLayout, cellType)

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

    for(tileId <- 0 until builder.rasterSeqs.length) {
      val rasterPath = new Path(outputDir, f"tile-${tileId}%05d")
      fs.mkdirs(rasterPath)

      val timeSeriesRasters = builder.rasterSeqs(tileId)

      val key = new TimeIdWritable()

      val writers = openWriters(rasterPath, partitioner.numPartitions)

      try {
        timeSeriesRasters.foreach {
          case TimeSeriesRaster(timeId, tile) => {
            key.set(timeId.timeId)
            writers(partitioner.getPartition(timeId.timeId)).append(key, ArgWritable.fromTile(tile))
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
