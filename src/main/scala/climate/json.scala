package climate

import geotrellis._
import geotrellis.raster.TileLayout

import spray.json._

package object json {
  implicit object ExtentFormat extends RootJsonFormat[Extent] {
    def write(extent: Extent) = 
      JsObject(
        "xmin" -> JsNumber(extent.xmin),
        "ymin" -> JsNumber(extent.ymin),
        "xmax" -> JsNumber(extent.xmax),
        "ymax" -> JsNumber(extent.ymax)
      )

    def read(value: JsValue): Extent =
      value.asJsObject.getFields("xmin", "ymin", "xmax", "ymax") match {
        case Seq(JsNumber(xmin), JsNumber(ymin), JsNumber(xmax), JsNumber(ymax)) =>
          Extent(xmin.toDouble, ymin.toDouble, xmax.toDouble, ymax.toDouble)
        case _ =>
          throw new DeserializationException("Extent [xmin,ymin,xmax,ymax] expected")
      }
  }

  implicit object RasterExtentFormat extends RootJsonFormat[RasterExtent] {
    def write(rasterExtent: RasterExtent) = 
      JsObject(
        "extent" -> rasterExtent.extent.toJson,
        "cols" -> JsNumber(rasterExtent.cols),
        "rows" -> JsNumber(rasterExtent.rows),
        "cellwidth" -> JsNumber(rasterExtent.cellwidth),
        "cellheight" -> JsNumber(rasterExtent.cellheight)
      )

    def read(value: JsValue): RasterExtent =
      value.asJsObject.getFields("extent", "cols", "rows", "cellwidth", "cellheight") match {
        case Seq(extent, JsNumber(cols), JsNumber(rows), JsNumber(cellwidth), JsNumber(cellheight)) =>
          val ext = extent.convertTo[Extent]
          RasterExtent(ext, cellwidth.toDouble, cellheight.toDouble, cols.toInt, rows.toInt)
        case _ =>
          throw new DeserializationException("RasterExtent expected.")
      }
  }

  implicit object TileLayoutFormat extends RootJsonFormat[TileLayout] {
    def write(tileLayout: TileLayout) =
      JsObject(
        "tileCols" -> JsNumber(tileLayout.tileCols), 
        "tileRows" -> JsNumber(tileLayout.tileRows), 
        "pixelCols" -> JsNumber(tileLayout.pixelCols), 
        "pixelRows" -> JsNumber(tileLayout.pixelRows)
      )

    def read(value: JsValue): TileLayout =
      value.asJsObject.getFields("tileCols", "tileRows", "pixelCols", "pixelRows") match {
        case Seq(JsNumber(tileCols), JsNumber(tileRows), JsNumber(pixelCols), JsNumber(pixelRows)) =>
          TileLayout(tileCols.toInt, tileRows.toInt, pixelCols.toInt, pixelRows.toInt)
        case _ =>
          throw new DeserializationException("TileLayout expected.")
      }
  }

  implicit object TimeSeriesMetadataFormat extends RootJsonFormat[TimeSeriesMetadata] {
    def write(metadata: TimeSeriesMetadata) = 
      JsObject(
        "timeCount" -> JsNumber(metadata.timeCount),
        "splits" -> JsArray(metadata.splits.map(JsNumber(_)).toList),
        "rasterExtent" -> metadata.rasterExtent.toJson,
        "tileLayout" -> metadata.tileLayout.toJson,
        "rasterType" -> JsNumber(RasterType.toAwtType(metadata.rasterType))
      )

    def read(value: JsValue): TimeSeriesMetadata = 
      value.asJsObject.getFields("timeCount", "splits", "rasterExtent", "tileLayout", "rasterType") match {
        case Seq(JsNumber(timeCount), splits: JsArray, rasterExtent, tileLayout, JsNumber(rasterType)) =>
          val spl = 
            splits.elements.map { s =>
              s match {
                case JsNumber(num) => num.toInt
                case _ => throw new DeserializationException("Splits must be numbers.")
              }
            }
          val re = rasterExtent.convertTo[RasterExtent]
          val tl = tileLayout.convertTo[TileLayout]
          val rt = RasterType.fromAwtType(rasterType.toInt)
          TimeSeriesMetadata(timeCount.toInt, spl.toArray, re, tl, rt)
        case _ => throw new DeserializationException("TimeSeriesMetadata expected.")
      }
  }
}
