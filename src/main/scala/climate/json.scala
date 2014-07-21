package climate

import geotrellis.raster._
import geotrellis.feature.Extent

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
        "extent" -> metadata.extent.toJson,
        "tileLayout" -> metadata.tileLayout.toJson,
        "cellType" -> JsNumber(CellType.toAwtType(metadata.cellType))
      )

    def read(value: JsValue): TimeSeriesMetadata = 
      value.asJsObject.getFields("timeCount", "splits", "extent", "tileLayout", "cellType") match {
        case Seq(JsNumber(timeCount), splits: JsArray, extent, tileLayout, JsNumber(cellType)) =>
          val spl = 
            splits.elements.map { s =>
              s match {
                case JsNumber(num) => num.toInt
                case _ => throw new DeserializationException("Splits must be numbers.")
              }
            }
          val e = extent.convertTo[Extent]
          val tl = tileLayout.convertTo[TileLayout]
          val ct = CellType.fromAwtType(cellType.toInt)
          TimeSeriesMetadata(timeCount.toInt, spl.toArray, e, tl, ct)
        case _ => throw new DeserializationException("TimeSeriesMetadata expected.")
      }
  }
}
