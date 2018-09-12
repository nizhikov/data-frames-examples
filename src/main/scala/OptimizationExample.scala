import org.apache.spark.sql.ignite.IgniteSparkSession
import org.apache.log4j.{Level, Logger}

/**
  */
object OptimizationExample extends App {
    private val CONFIG = "/data/ignite-config.xml"

    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache").setLevel(Level.ERROR)

    val session = IgniteSparkSession.builder()
        .appName("Spark Ignite catalog example")
        .master("local")
        .config("spark.executor.instances", "2")
        .igniteConfig(CONFIG)
        .getOrCreate()

    println("")
    println(">>> Select plan")
    println("")

    val playerByAcceleration = session.sql("""
        SELECT
            DISTINCT p.player_name, pa.overall_rating
        FROM
            player p JOIN
            player_attr pa ON p.player_api_id = pa.player_api_id
        WHERE pa.overall_rating IS NOT NULL
        ORDER BY pa.overall_rating DESC
    """)

    playerByAcceleration.explain()

    println("")
    println(">>> Player ordered by rating")
    println("")

    playerByAcceleration.show(10)

    session.close()
}
