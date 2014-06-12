package climate

import climate.json._
import spray.json._

import geotrellis._
import geotrellis.raster.TileLayout

import org.scalatest._

class JsonSpec extends FlatSpec with ShouldMatchers {
  "json package" should "parse Extent" in {
    val expected = Extent(-13.13, 23.2323, 600.600600, 44.0)
    val json = """{ "xmin" : -13.13, "ymin" : 23.2323, "xmax" : 600.600600, "ymax" : 44.0 }"""
    json.parseJson.convertTo[Extent] should be (expected)
  }

  "json package" should "parse RasterExtent" in {
    val extent = Extent(-3.13, 23.0, 6.6, 41.0)
    val cols = 13
    val rows = 23
    val cellwidth = (extent.xmax - extent.xmin) / cols.toDouble
    val cellheight = (extent.ymax - extent.ymin) / rows.toDouble
    val expected = RasterExtent(extent, cellwidth, cellheight, cols, rows)
    val json = s"""{ 
               | "extent" : { "xmin" : -3.13, "ymin" : 23, "xmax" : 6.6, "ymax" : 41.0 },
               | "cols" : 13,
               | "rows" : 23,
               | "cellwidth" : $cellwidth,
               | "cellheight" : $cellheight
               |}""".stripMargin

    json.parseJson.convertTo[RasterExtent] should be (expected)
  }

  "json package" should "parse TileLayout" in {
    val expected = TileLayout(2, 4, 7, 6)
    val json = s"""{ "tileCols" : 2, "tileRows" : 4, "pixelCols" : 7, "pixelRows" : 6 }"""

    json.parseJson.convertTo[TileLayout] should be (expected)
  }


  "json package" should "parse TimeSeriesMetadata" in {
    val extent = Extent(-3.13, 23.0, 6.6, 41.0)
    val cols = 14
    val rows = 24
    val cellwidth = (extent.xmax - extent.xmin) / cols.toDouble
    val cellheight = (extent.ymax - extent.ymin) / rows.toDouble
    val rasterExtent = RasterExtent(extent, cellwidth, cellheight, cols, rows)

    val splits = Array(100, 200, 400, 600)
    val rasterType = TypeInt

    val tileLayout = TileLayout(2, 4, 7, 6)

    val expected = TimeSeriesMetadata(1000, splits, rasterExtent, tileLayout, rasterType)

    val json = s"""{
               | "timeCount" : 1000,
               | "splits" : [ 100, 200, 400, 600 ],
               | "rasterExtent" : { 
               |   "extent" : { "xmin" : -3.13, "ymin" : 23, "xmax" : 6.6, "ymax" : 41.0 },
               |   "cols" : 14,
               |   "rows" : 24,
               |   "cellwidth" : $cellwidth,
               |   "cellheight" : $cellheight
               |  },
               |  "tileLayout" : { "tileCols" : 2, "tileRows" : 4, "pixelCols" : 7, "pixelRows" : 6 },
               |  "rasterType" : ${RasterType.toAwtType(TypeInt)}
               |}""".stripMargin

    val actual = json.parseJson.convertTo[TimeSeriesMetadata]
    println(actual.splits.toSeq)
    println(expected.splits.toSeq)
    actual should be (expected)
  }

  "json package" should "write and parse TimeSeriesMetadata" in {
    val extent = Extent(-3.13, 23.0, 6.6, 41.0)
    val cols = 13
    val rows = 23
    val cellwidth = (extent.xmax - extent.xmin) / cols.toDouble
    val cellheight = (extent.ymax - extent.ymin) / rows.toDouble
    val rasterExtent = RasterExtent(extent, cellwidth, cellheight, cols, rows)
    val tileLayout = TileLayout(2, 4, 7, 6)

    val splits = Array(100, 200, 400, 600)
    val rasterType = TypeInt

    val tsm = TimeSeriesMetadata(1000, splits, rasterExtent, tileLayout, rasterType)

    tsm.toJson.compactPrint.parseJson.convertTo[TimeSeriesMetadata] should be (tsm)
  }
}
