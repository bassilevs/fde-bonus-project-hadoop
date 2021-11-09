import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object ReturnTrips {
  def compute(trips: Dataset[Row], dist: Double, spark: SparkSession): Dataset[Row] = {

    import spark.implicits._

    val earthR = 6371

    def distance(lat1: Column, lon1: Column, lat2: Column, lon2: Column): Column = {
      val f1 = radians(lat1)
      val f2 = radians(lat2)
      val deltaF = radians(lat2 - lat1)
      val deltaL = radians(lon2 - lon1)

      val a = sin(deltaF / 2) * sin(deltaF / 2) + cos(f1) * cos(f2) * sin(deltaL / 2) * sin(deltaL / 2)
      val c = atan2(sqrt(a), sqrt(-a + 1)) * 2
      c * earthR * 1000
    }

    val buck_width_lat = dist * 360 / (earthR * 1000 * 2 * Math.PI)

    val convTrips = trips.withColumn("tpep_pickup_datetime", unix_timestamp($"tpep_pickup_datetime"))
      .withColumn("tpep_dropoff_datetime", unix_timestamp($"tpep_dropoff_datetime"))
    // Buckets
    val timeBuck = convTrips
      .withColumn("pickup_date_buck", floor($"tpep_pickup_datetime" / 28800))
      .withColumn("dropoff_date_buck", floor($"tpep_dropoff_datetime" / 28800))
      .withColumn("pickup_lat_buck", floor($"pickup_latitude" / buck_width_lat))
      .withColumn("dropoff_lat_buck", floor($"dropoff_latitude" / buck_width_lat)).cache()

    // Neighbours
    val timeBuckNeighbours = timeBuck
      .withColumn("pickup_date_buck", explode(array($"pickup_date_buck" - 1, $"pickup_date_buck")))
      .withColumn("pickup_lat_buck", explode(array($"pickup_lat_buck" - 1, $"pickup_lat_buck", $"pickup_lat_buck" + 1)))
      .withColumn("dropoff_lat_buck", explode(array($"dropoff_lat_buck" - 1, $"dropoff_lat_buck", $"dropoff_lat_buck" + 1)))

    val result = timeBuck.as("a").join(timeBuckNeighbours.as("b"),
      $"a.dropoff_date_buck" === $"b.pickup_date_buck" &&
        $"a.pickup_lat_buck" === $"b.dropoff_lat_buck" &&
        $"a.dropoff_lat_buck" === $"b.pickup_lat_buck" &&
        $"a.tpep_dropoff_datetime" < $"b.tpep_pickup_datetime" &&
        $"b.tpep_pickup_datetime" - $"a.tpep_dropoff_datetime" < 28800 &&
        distance($"a.dropoff_latitude", $"a.dropoff_longitude", $"b.pickup_latitude", $"b.pickup_longitude") < dist &&
        distance($"b.dropoff_latitude", $"b.dropoff_longitude", $"a.pickup_latitude", $"a.pickup_longitude") < dist)
    result
  }
}
