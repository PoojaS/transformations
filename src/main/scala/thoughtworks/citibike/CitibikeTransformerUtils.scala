package thoughtworks.citibike

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  implicit class StringDataset(val dataSet: Dataset[Row]) {


    def computeDistances(spark: SparkSession) = {

      import org.apache.spark.sql.functions.callUDF
      import spark.implicits._
      spark.udf.register("computeDistance", computeDistance)

      val frame = dataSet.withColumn("distance", callUDF("computeDistance",
        $"start_station_latitude", $"end_station_latitude"
        , $"start_station_longitude", $"end_station_longitude"))
      frame
    }

    val computeDistance = (startStationLatitude: Double, endStationLatitude: Double,
                           startStationLongitude: Double, endStationLongitude: Double) => {
      import scala.math.{atan2, cos, pow, sin, sqrt, toRadians}

      val latitudeDifference = toRadians(endStationLatitude - startStationLatitude)
      val longitudeDifference = toRadians(endStationLongitude - startStationLongitude)

      val a =
        pow(sin(latitudeDifference / 2), 2) +
          cos(toRadians(startStationLatitude)) * cos(toRadians(endStationLatitude)) *
            pow(sin(longitudeDifference / 2), 2)

      BigDecimal((2 * atan2(sqrt(a), sqrt(1 - a)) * EarthRadiusInM) / MetersPerMile)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    }


  }

}
