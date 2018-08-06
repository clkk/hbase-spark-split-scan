package scanner

import java.util.Date

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ScannerJob {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val session = SparkSession.builder().appName("Scan with multiple TableSplit").master("local").getOrCreate()
    val sc = session.sparkContext
    val hadoopConfig = sc.getConf
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val sqlContext = session.sqlContext
    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, "hbase_table")
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "cf")
    conf.set(TableInputFormat.SCAN_CACHEBLOCKS, "false")

    conf.set("table", "hbase_table")
    conf.set("hbase.zookeeper.quorum", "namenode")
    conf.set("logical.scan.start", "start_key")
    conf.set("logical.scan.stop", "end_key")
    conf.set("logical.scan.randChars", "salting_char_array")
    conf.set("logical.scan.batchCount", "batch_count")

    val admin = ConnectionFactory.createConnection(conf)
    val startDate: Long = new Date().getTime
    println(admin.getTable(TableName.valueOf("hbase_table")))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[MultiSplitTableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val first = hBaseRDD.count

    val endDate: Long = new Date().getTime

    println("count: " + first + ", total time: " + (endDate - startDate))
    println("** Read Done **")
    sc.stop()
  }
}
