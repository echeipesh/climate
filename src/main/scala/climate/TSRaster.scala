package climate

import climate.json._
import spray.json._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.formats._
import geotrellis.spark.utils.HdfsUtils

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import org.apache.hadoop.fs.Path

class TSRaster(metadata: TimeSeriesMetadata, val rdds: Seq[TSRasterRDD]) {
  val (cols, rows) = (metadata.tileLayout.pixelCols, metadata.tileLayout.pixelRows)
  def sum: Tile =
    CompositeTile(
      rdds.par.map(_.sum(cols, rows)).toList,
      metadata.tileLayout
    ).toArrayTile

  def timeCount: Int = 
    rdds.head.count.toInt
    //metadata.timeCount

  def filter(f: TimeSeriesRaster => Boolean): TSRaster =
    new TSRaster(metadata, rdds.map(rdd => TSRasterRDD.fromRdd(rdd.filter(f))))

  def cache: TSRaster =
    new TSRaster(metadata, rdds.map(rdd => TSRasterRDD.fromRdd(rdd.cache)))
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
          metadata.cellType,
          tileLayout.pixelCols,
          tileLayout.pixelRows
        ).toTSRasterRDD
      }

    new TSRaster(metadata, rdds)
  }
}
