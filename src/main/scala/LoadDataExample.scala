
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
  */
object LoadDataExample extends App {
    private val CFG_PATH = "/data/ignite-config.xml"

    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
        .appName("Spark Ignite catalog example")
        .master("spark://172.17.0.5:7077")
        .getOrCreate()

    def loadTable(file: String, tableName: String, pk: String): Unit = {
        val tbl = spark.read.option("multiline", true).json(file)

        tbl.write.
            format(FORMAT_IGNITE).
            option(OPTION_CONFIG_FILE, CFG_PATH).
            option(OPTION_TABLE, tableName).
            option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, pk).
            save

        println(s">>> ${tableName.toUpperCase} loaded.")
    }

    loadTable("/data/Country.json", "country", "id")
    loadTable("/data/Match.json", "tmatch", "id")
    loadTable("/data/Player.json", "player", "id")
    loadTable("/data/Player_Attributes.json", "player_attr", "id")
    loadTable("/data/Team.json", "team", "id")
    loadTable("/data/Team_Attributes.json", "team_attr", "id")

    spark.close()
}
