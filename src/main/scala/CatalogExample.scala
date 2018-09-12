import org.apache.spark.sql.ignite.IgniteSparkSession
import org.apache.log4j.{Level, Logger}

/**
  */
object CatalogExample extends App {
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
    println(">>> Known tables")
    println("")

    session.catalog.listTables().show()

    println("")
    println(">>> Country table schema")
    println("")

    session.catalog.listColumns("country").show()

    println("")
    println(">>> Country table")
    println("")

    session.sql("SELECT * FROM Country").show()

    session.close()
}
