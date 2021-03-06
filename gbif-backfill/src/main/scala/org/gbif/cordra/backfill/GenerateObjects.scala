package org.gbif.cordra.backfill

import java.io.File

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.{Lists, Maps}
import com.google.gson.{Gson, JsonObject, JsonParser}
import net.cnri.cordra.api.CordraObject
import net.cnri.cordra.indexer.elasticsearch.DocumentBuilderElasticsearch
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import com.databricks.spark.avro._

import scala.util.parsing.json.JSONObject

/**
 * Takes the GBIF data and generates Corda objects prepared to pass into Elasticsearch.
 * The output is stored in Avro, suitable for subsequent indexing in ES and loading to HBase.
 * By storing these in the intermediate files, we can rerun the import processes, and ensure that
 * both data stores have the same timestamps and transaction IDs.
 */
object GenerateObjects {
  // all properties are supplied as main arguments to simplify running
  val usage = """
    Usage: Cluster \
      [--hive-db hiveDatabase] \
      [--target-dir targetDir]
  """

  def main(args: Array[String]): Unit = {

    val parsedArgs = checkArgs(args) // sanitize input
    assert(parsedArgs.size==2, usage)
    System.err.println("Configuration: " + parsedArgs) // Spark friendly logging use

    val hiveDatabase = parsedArgs.get('hiveDatabase).get
    val targetDir = parsedArgs.get('targetDir).get

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Create Cordra Objects")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    import spark.implicits._

    spark.sql("use " + hiveDatabase)

    val occurrences = sql(SQL_OCCURRENCE)

    val out = occurrences.map(o => {
      val builder = new DocumentBuilderElasticsearch(false, null)

      // map occurrence to a JSON (TODO Does this support DwC-A extensions properly?)
      val m = o.getValuesMap(o.schema.fieldNames)
      val filteredMap = m.filter(x => x._2 != null) // remove null values for CordraObject
      val j = JSONObject(filteredMap).toString()

      val co = new CordraObject
      co.id = "20.5000.123.TEST/" + o.getAs("gbifId").toString
      co.`type`="Occurrence"

      val gson = new Gson

      co.content = JsonParser.parseString(j)
      co.metadata = new CordraObject.Metadata
      co.metadata.createdBy = "admin"
      co.metadata.createdOn = 1643375628829l
      co.metadata.txnId = 1643375628829011l
      co.metadata.modifiedBy = "admin"
      co.metadata.modifiedOn = 1643375628829l
      co.metadata.internalMetadata = new JsonObject

      val doc = builder.build(co, false, Maps.newHashMap[String, JsonNode], Lists.newArrayList[Runnable])
      val esDoc = gson.toJson(doc)
      val coJson = gson.toJson(co)

      (coJson, esDoc)

    }).toDF("co", "es")

    out.write.avro(targetDir)
  }

  /**
   * Sanitizes application arguments.
   */
  private def checkArgs(args: Array[String]) : Map[Symbol, String] = {
    assert(args != null, usage)

    def nextOption(map : Map[Symbol, String], list: List[String]) : Map[Symbol, String] = {
      list match {
        case Nil => map
        case "--hive-db" :: value :: tail =>
          nextOption(map ++ Map('hiveDatabase -> value), tail)
        case "--target-dir" :: value :: tail =>
          nextOption(map ++ Map('targetDir -> value), tail)
        case option :: tail => println("Unknown option "+option)
          System.exit(1)
          map
      }
    }
    nextOption(Map(), args.toList)
  }
}
