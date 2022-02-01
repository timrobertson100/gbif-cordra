package org.gbif.cordra.backfill

import com.databricks.spark.avro._
import com.google.gson.{Gson, JsonParser}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.{ES_INPUT_JSON, ES_NODES}


/**
 * Takes the generated objects stored in Avro and indexes in ES.
 */
object IndexToES {
  // all properties are supplied as main arguments to simplify running
  val usage = """
    Usage: Cluster \
      [--source-dir sourceDir] \
      [--es-nodes esNodes] \
      [--es-port esPort] \
      [--es-index esIndex]
  """

  def main(args: Array[String]): Unit = {

    val parsedArgs = checkArgs(args) // sanitize input
    assert(parsedArgs.size==4, usage)
    System.err.println("Configuration: " + parsedArgs) // Spark friendly logging use

    val sourceDir = parsedArgs.get('sourceDir).get
    val esNodes = parsedArgs.get('esNodes).get
    val esPort = parsedArgs.get('esPort).get
    val esIndex = parsedArgs.get('esIndex).get

    val spark = SparkSession
      .builder()
      .appName("Index Cordra Objects")
      .config("spark.es.nodes", esNodes)
      .config("spark.es.port", esPort)
      .getOrCreate()
    import spark.implicits._

    val cfg = Map(
      ("es.mapping.id", "id")
    )

    val df = spark.read.avro(sourceDir)

    df.map(r => {
      val json = r.getString(1) // co,es are the fields

      // cordra puts everything into arrays such as "id":[123]
      val gbifId = JsonParser.parseString(json).getAsJsonObject.get("id").getAsJsonArray.get(0).getAsString
      ("\"" + gbifId + "\"", json) // quotes required for ES bulk load to see the string

    }).rdd.repartition(80).saveToEsWithMeta(esIndex, Map(
      ES_INPUT_JSON -> true.toString
    ))

  }

  /**
   * Sanitizes application arguments.
   */
  private def checkArgs(args: Array[String]) : Map[Symbol, String] = {
    assert(args != null, usage)

    def nextOption(map : Map[Symbol, String], list: List[String]) : Map[Symbol, String] = {
      list match {
        case Nil => map
        case "--source-dir" :: value :: tail =>
          nextOption(map ++ Map('sourceDir -> value), tail)
        case "--es-nodes" :: value :: tail =>
          nextOption(map ++ Map('esNodes -> value), tail)
        case "--es-port" :: value :: tail =>
          nextOption(map ++ Map('esPort -> value), tail)
        case "--es-index" :: value :: tail =>
          nextOption(map ++ Map('esIndex -> value), tail)
        case option :: tail => println("Unknown option "+option)
          System.exit(1)
          map
      }
    }
    nextOption(Map(), args.toList)
  }
}
