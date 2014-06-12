package climate

import geotrellis._
import geotrellis.raster.TileLayout

case class TimeSeriesMetadata(
  timeCount: Int,
  splits: Array[Int], 
  rasterExtent: RasterExtent, 
  tileLayout: TileLayout, 
  rasterType: RasterType
) {
  lazy val resolutionLayout = tileLayout.getResolutionLayout(rasterExtent)
  def partitioner = new TimeSeriesPartitioner(splits)

  override def equals(o: Any): Boolean =
    o match {
      case other: TimeSeriesMetadata =>
        toHashable == other.toHashable
      case _ => false
    }

  override def hashCode = toHashable.hashCode

  private lazy val toHashable = (
    timeCount,
    java.util.Arrays.hashCode(splits.map(_.hashCode)),
    rasterExtent, 
    tileLayout, 
    rasterType
  )
}
