import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.time._

object Main {
  private def fakeData(): Map[String, Map[Long, Double]] = {
    val letters = 'B' to 'Y'
    val symbols = letters.flatMap(c1 => letters.flatMap(c2 => letters.map(c3 => f"$c1$c2$c3")))
    val startOfDay: Long = ZonedDateTime.now(ZoneOffset.UTC)
      .withHour(0)
      .withMinute(0)
      .withSecond(0)
      .withNano(0)
      .toEpochSecond
    val timestamps = (0 until 6 * 60 * 60 by 60).map(startOfDay + _)

    import scala.util.Random
    symbols.map(_ -> timestamps.map(_ -> (100 + (Random.nextInt(10000) / 100.0))).toMap).toMap
  }

  private def round2Decimals(n: Double): Double = {
    BigDecimal(n).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def main(args: Array[String]): Unit = {

    val dataset: Map[String, Map[Long, Double]] = fakeData()

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Luxoft Scala Task")
      .getOrCreate()

    val rdd: RDD[((String, ZonedDateTime), Map[String, Double])] = spark.sparkContext
      .parallelize(dataset.toSeq)
      .flatMap { case (symbol: String, m: Map[Long, Double]) =>
        m.toSeq.map { case (timestamp, value) =>
          (symbol, ZonedDateTime
            .ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.of("UTC"))
            .withMinute(0)
          ) -> Map(
            "count" -> 1.0,
            "min" -> value,
            "max" -> value,
            "sum" -> value,
          )
        }
      }
      .reduceByKey((map1, map2) => Map(
        "count" -> (map1("count") + map2("count")),
        "min" -> List(map1("min"), map2("min")).min,
        "max" -> List(map1("max"), map2("max")).max,
        "sum" -> (map1("sum") + map2("sum")),
      ))

    rdd.collectAsMap().foreach({
      case (key: (String, ZonedDateTime), value: Map[String, Double]) =>
        println(f"${key._1} @ ${key._2}: MIN=${round2Decimals(value("min"))} MAX=${round2Decimals(value("max"))} AVG=${round2Decimals(value("sum") / value("count"))}")
    })

    spark.stop()
  }
}