package org.gbif.cordra.backfill

import com.databricks.spark.avro._
import com.google.gson.{JsonElement, JsonParser}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partitioner

/**
 * Takes the generated objects stored in Avro and generates HFiles suitable for bulk loading.
 */
object GenerateHFiles {
  // all properties are supplied as main arguments to simplify running
  val usage = """
    Usage: Cluster \
      [--source-dir sourceDir] \
      [--hbase-table tableName] \
      [--hbase-regions numberOfRegions] \
      [--hbase-zk zookeeperEnsemble] \
      [--hfile-dir directoryForHFiles]
  """

  /**
   * Reads the salt from the encoded key structure.
   */
  class SaltPartitioner(partitionCount: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      // key is of format salt:objectId
      val k = key.asInstanceOf[String]
      Integer.valueOf(k.substring(0, k.indexOf(":")))
    }

    override def numPartitions: Int = partitionCount
  }

  def main(args: Array[String]): Unit = {

    val parsedArgs = checkArgs(args) // sanitize input
    assert(parsedArgs.size==5, usage)
    System.err.println("Configuration: " + parsedArgs) // Spark friendly logging use

    val sourceDir = parsedArgs.get('sourceDir).get
    val hbaseTable = parsedArgs.get('hbaseTable).get
    val hbaseRegions = parsedArgs.get('hbaseRegions).get.toInt
    val hbaseZK = parsedArgs.get('hbaseZK).get
    val hfileDir = parsedArgs.get('hfileDir).get

    val spark = SparkSession
      .builder()
      .appName("Index Cordra Objects")
      .getOrCreate()
    import spark.implicits._

    val df = spark.read.avro(sourceDir)

    // extract the object ID, salt it, and partition to align with the HBase regions
    val keyedObjects = df.map(r => {
      //val json = r.getString(df.schema.fieldNames.indexOf("co"))
      val json = r.getString(0) // co is first

      // cordra puts everything into arrays such as "id":[123]
      val id = JsonParser.parseString(json).getAsJsonObject.get("id").getAsString
      val salt = (id.hashCode() & 0xfffffff) % hbaseRegions
      val saltedRowKey = salt + ":" + id

      (saltedRowKey, json)

    }).rdd

    val sortedObjects = keyedObjects.repartitionAndSortWithinPartitions(new SaltPartitioner(hbaseRegions)).map(cell => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(cell._1))
      val row = new KeyValue(Bytes.toBytes(cell._1), // key
        Bytes.toBytes("m"), // column family
        Bytes.toBytes("m"), // cell
        Bytes.toBytes(cell._2) // cell value is the object
      )
      (k, row)
    })

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hbaseZK);
    // NOTE: job creates a copy of the conf
    val job = new Job(conf,"Ignored") // since we don't submit MR
    job.setJarByClass(this.getClass)
    val table = new HTable(conf, hbaseTable)
    HFileOutputFormat2.configureIncrementalLoad(job, table);
    val conf2 = job.getConfiguration // important

    sortedObjects.saveAsNewAPIHadoopFile(hfileDir, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], conf2)
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
        case "--hbase-table" :: value :: tail =>
          nextOption(map ++ Map('hbaseTable -> value), tail)
        case "--hbase-regions" :: value :: tail =>
          nextOption(map ++ Map('hbaseRegions -> value), tail)
        case "--hbase-zk" :: value :: tail =>
          nextOption(map ++ Map('hbaseZK -> value), tail)
        case "--hfile-dir" :: value :: tail =>
          nextOption(map ++ Map('hfileDir -> value), tail)
        case option :: tail => println("Unknown option "+option)
          System.exit(1)
          map
      }
    }
    nextOption(Map(), args.toList)
  }
}
