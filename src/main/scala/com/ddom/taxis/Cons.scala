package com.ddom.taxis

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.jsoup.Jsoup

import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}

object Cons {

  // SPARK
  private val APP_NAME  = "stream-taxis"
  private val BATCH_DURATION  = Seconds(5) //sliding interval
  private val WINDOW_DURATION = Seconds(30) //it takes about 1s of real time for kafka to publish 1m of timestamps //to simulate a window of 30m in timestamps time, the window needs to have 30s (real time)

  // KAFKA
  private val TOPIC = "debs"
  private val BOOTSTRAP_SERVERS = "localhost:9092"
  private val AUTO_OFFSET_RESET = "latest"
  private val ENABLE_AUTO_COMMIT = false: java.lang.Boolean
  private val COMMA_DELIMITER = ","

  // FRONTEND
  private val UPDATE_DELAY = Seconds(5) //same as the batch/sliding interval
  private val FRONTEND_PATH = "/taxis-app/"
  private val TEMPLATE_FILE = "Map.html"
  private val FREQ_ROUTES_FILE = "FreqRoutes.html"
  private val PROFIT_AREAS_FILE = "ProfitAreas.html"

  case class KRec(
                   medallion:String, hack_license:String,
                   pickup_datetime:String, dropoff_datetime:String,
                   trip_time_in_secs:Int, trip_distance:Double,
                   pickup_longitude:Double, pickup_latitude:Double,
                   dropoff_longitude:Double, dropoff_latitude:Double,
                   payment_type:String, fare_amount:Double, surcharge:Double,
                   mta_tax:Double, tip_amount:Double,
                   tolls_amount:Double, total_amount:Double,
                   start_cell_id:String, start_cell_x:Int, start_cell_y:Int,
                   end_cell_id:String, end_cell_x:Int, end_cell_y:Int,
                   route:String, fare_plus_tip:Double
                 )

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName(APP_NAME)
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, BATCH_DURATION)

    val topics = Array(TOPIC)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, getKafkaParams)
    )

    val lines = stream.map(rec => rec.value())
    val parsed = lines.map(line => parse(line))
    val filtered = parsed.filter(rec => rec.start_cell_x >= 0 && rec.start_cell_x <= 300)
      .filter(rec => rec.start_cell_y >= 0 && rec.start_cell_y <= 300)
      .filter(rec => rec.end_cell_x >= 0 && rec.end_cell_x <= 300)
      .filter(rec => rec.end_cell_y >= 0 && rec.end_cell_y <= 300)
    filtered.window(WINDOW_DURATION).foreachRDD(rdd => computeWindow(spark, rdd))

    ssc.start()
    while (true) {
      var time: Long = System.currentTimeMillis()
      Thread.sleep(UPDATE_DELAY.milliseconds)
      try {
        val query1 = spark.sqlContext.table("FirstQueryView")
        editFrontend(FREQ_ROUTES_FILE, query1.collect())
        //query1.show()

        val query2 = spark.sqlContext.table("SecondQueryView")
        editFrontend(PROFIT_AREAS_FILE, query2.collect())
        //query2.show()

        println("Elapsed time since last update: " + ((System.currentTimeMillis() - time) / 1000) + "s     ")
        time = System.currentTimeMillis()
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  def getKafkaParams: Map[String,Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BOOTSTRAP_SERVERS,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> APP_NAME,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> AUTO_OFFSET_RESET,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> ENABLE_AUTO_COMMIT
    )
  }

  def parse(rec:String): KRec = {
    val rs = rec.split(COMMA_DELIMITER)
    val (scId, scX, scY) = getCell(rs(7).toDouble,rs(6).toDouble)
    val (ecId, ecX, ecY) = getCell(rs(9).toDouble,rs(8).toDouble)
    val route = scId+";"+ecId
    val fare_plus_tip = rs(11).toDouble + rs(14).toDouble

    KRec(
      rs(0), rs(1),
      rs(2), rs(3),
      rs(4).toInt, rs(5).toDouble,
      rs(6).toDouble, rs(7).toDouble,
      rs(8).toDouble, rs(9).toDouble,
      rs(10), rs(11).toDouble, rs(12).toDouble,
      rs(13).toDouble, rs(14).toDouble,
      rs(15).toDouble, rs(16).toDouble,
      scId, scX, scY,
      ecId, ecX, ecY,
      route, fare_plus_tip
    )
  }

  def getCell(lat2:Double, lng2:Double): (String,Int,Int) = {
    var res:String = ""
    val lat1:Double = 41.474937
    val lng1:Double = -74.913585

    //EAST COORDINATE
    var distLat:Double = 0
    var distLng:Double = deg2rad(lng2-lng1)
    var dist:Double = getDist(lat1, lat2, distLat, distLng)
    val gridLng:Int = ((dist/500) + 1).toInt
    res += gridLng + "."

    //SOUTH COORDINATE
    distLat = deg2rad(lat2-lat1)
    distLng = 0
    dist = getDist(lat1, lat2, distLat, distLng)
    val gridLat:Int = ((dist/500) + 1).toInt
    res += gridLat

    (res, gridLng, gridLat)
  }

  // get distance between two earth coordinate points
  def getDist(lat1:Double, lat2:Double, distLat:Double, distLng:Double): Double = {
    val earthRadius:Double = 6371
    val a = Math.sin(distLat/2) * Math.sin(distLat/2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(distLng/2) * Math.sin(distLng/2)
    val angle = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
    val dist = earthRadius * angle; //distance in km
    dist*1000 //distance in metres
  }

  def deg2rad(deg:Double): Double = {
    deg * (Math.PI/180)
  }

  // create sql tables with needed data
  def computeWindow(spark:SparkSession, rdd:RDD[KRec]): Unit = {
    spark.sqlContext.createDataFrame(rdd).createOrReplaceTempView("RddView")

    //query1: best routes
    val query1 = spark.sqlContext.sql("" +
      "SELECT route, count(*) as total, first(pickup_latitude) as pLat, first(pickup_longitude) as pLng, first(dropoff_latitude) as dLat, first(dropoff_longitude) as dLng " +
      "FROM RddView " +
      "GROUP BY route " +
      "ORDER BY 2 DESC").limit(10)
    query1.createOrReplaceTempView("FirstQueryView")

    //query2: area profit
    val areaProfit = spark.sqlContext.sql("" +
      "SELECT start_cell_id as area, first(pickup_latitude) as lat, first(pickup_longitude) as lng, percentile_approx(fare_plus_tip, 0.5) as medianprofit " +
      "FROM RddView " +
      "GROUP BY start_cell_id " +
      "ORDER BY 2 DESC")
    areaProfit.createOrReplaceTempView("AreaProfitView")

    //query2: empty taxis
    val taxiEndCells = spark.sqlContext.sql("" +
      "SELECT end_cell_id as area, count(*) as tota " +
      "FROM RddView " +
      "GROUP BY end_cell_id")
    taxiEndCells.createOrReplaceTempView("TaxiEndCells")
    val taxiStartCells = spark.sqlContext.sql("" +
      "SELECT start_cell_id as area, count(*) as totb " +
      "FROM RddView " +
      "GROUP BY start_cell_id")
    taxiStartCells.createOrReplaceTempView("TaxiStartCells")
    val areaTaxisRaw = spark.sqlContext.sql("" +
      "SELECT a.area as area, a.tota as tota, b.totb as totb " +
      "FROM TaxiEndCells as a LEFT JOIN TaxiStartCells as b ON a.area = b.area")
    val areaTaxis = areaTaxisRaw.na.fill(0) //end or start cell without results have 0 taxis
    areaTaxis.createOrReplaceTempView("AreaTaxisView")
    val emptyTaxisView = spark.sqlContext.sql("" +
      "SELECT area, (tota-totb) as ntaxis " +
      "FROM AreaTaxisView")
    emptyTaxisView.createOrReplaceTempView("EmptyTaxisView")

    //query2: profitability
    val query2 = spark.sqlContext.sql("" +
      "SELECT a.area, b.lat, b.lng, a.ntaxis, b.medianprofit, (b.medianprofit / a.ntaxis) as profitability " +
      "FROM EmptyTaxisView as a INNER JOIN AreaProfitView as b ON a.area = b.area " +
      "ORDER BY 6 DESC").limit(10)
    query2.createOrReplaceTempView("SecondQueryView")
  }

  def editFrontend(edit_file:String, rows:Array[Row]): Unit = {
    val input = new File(FRONTEND_PATH+TEMPLATE_FILE)
    val doc = Jsoup.parse(input, "UTF-8", "")
    val script = doc.select("script").last()

    script.append("\n")
    var j = 0
    for(t <- rows) {
      var line = ""
      if(edit_file.equals(FREQ_ROUTES_FILE))
        line = "coordsLines[" + j + "] = [[" + t.get(2) + "," + t.get(3) + "], [" + t.get(4) + ", " + t.get(5) + "]];\n"
      else line = "coordsMarkers[" + j + "] = L.latLng(" + t.get(1) + ", " + t.get(2) + ");\n"
      script.append(line)
      j+=1
    }
    if(edit_file.equals(FREQ_ROUTES_FILE))
      script.append("updateLines();\n")
    else script.append("updateMarkers();\n")

    val html = doc.html()
    var htmlWriter:BufferedWriter = null
    if(edit_file.equals(FREQ_ROUTES_FILE))
     htmlWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(FRONTEND_PATH+FREQ_ROUTES_FILE), "UTF-8"))
    else htmlWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(FRONTEND_PATH+PROFIT_AREAS_FILE), "UTF-8"))
    htmlWriter.write(html)
    htmlWriter.close()
  }
}