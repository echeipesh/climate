package climate

import geotrellis.raster._
import geotrellis.feature.Extent

case class TimeSeriesMetadata(
  timeCount: Int,
  splits: Array[Int], 
  extent: Extent,
  tileLayout: TileLayout, 
  cellType: CellType
) {
  lazy val resolutionLayout = TileExtents(extent, tileLayout)
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
    extent,
    tileLayout, 
    cellType
  )
}
