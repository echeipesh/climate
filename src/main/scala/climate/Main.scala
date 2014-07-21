package climate

import climate.json._
import spray.json._

import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
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

import java.lang.System.currentTimeMillis

object Benchmark {
  def apply[T](msg: String)(body: => T): T = {
    println(s"[BENCH] $msg - START ${currentTimeMillis -  1405716575000L - 185000L}")
    val start = currentTimeMillis
    val r = body
    val duration = currentTimeMillis - start
    println(s"[BENCH] $msg - END  ${currentTimeMillis - 1405716575000L - 185000L} - Took $duration ms")
    r
  }

  def apply(body: => Unit): Long = {
    val start = currentTimeMillis()
    body
    currentTimeMillis() - start
  }

  def apply(times:Int)(body: => Unit): Long = {
    var i = 0
    var totalDuration = 0L
    while(i < times) {
      totalDuration += apply(body)
      i += 1
    }
    totalDuration / i
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

      val start = TimeId(new org.joda.time.DateTime(2049,1,1,12,0,org.joda.time.DateTimeZone.UTC)).toInt
      val end = TimeId(new org.joda.time.DateTime(2050,1,1,12,0,org.joda.time.DateTimeZone.UTC)).toInt


      val tsr = TSRaster(outputDir, sparkContext)
// .filter { tsr => 
//         val i = tsr.timeId.toInt
//         i >= start && i <= end
//       }


      import geotrellis.raster._
      import geotrellis.raster.op.local._
      val s = (1 to 12).map { i => ArrayTile.empty(TypeDouble,180,180) }
      val d2 = 
        Benchmark {
          s.toSeq.localAdd
          ()
        }



      val duration = 
        Benchmark("TOTAL") {
          println("Start.")

          val start = currentTimeMillis()

          val rdd = tsr.rdds.head
          println("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")
          val results = new Array[Array[TimeSeriesRaster]](rdd.partitions.size)
          val func = (iter: Iterator[TimeSeriesRaster]) => Benchmark("ITERATOR") { iter.toArray }
          sparkContext.runJob(
            rdd, 
            (context: TaskContext, iter: Iterator[TimeSeriesRaster]) => { Benchmark("Do Func") { func(iter) } },
            0 until rdd.partitions.size, 
            false,
            (index: Int, res: Array[TimeSeriesRaster]) => { Benchmark("Set Result") { results(index) = res } }
          )
//          val sum = tsr.sum
//          println(s"(Min,Max) = ${sum.findMinMaxDouble}")//, count = $count")

//          val count = tsr.timeCount
//          val mean = sum.mapDouble(_/count)


          val duration = currentTimeMillis() - start


//          println(s"(Min,Max) = ${mean.findMinMaxDouble}, count = $count  ($duration ms)")


          println("End.")
        }
//      println(s"Took $duration milliseconds.")
      println(s"Isolated example: $d2 ms")
    }
    println("Done.")
  }
}
