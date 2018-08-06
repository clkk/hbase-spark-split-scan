package scanner

import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, RegionLocator, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSplit}
import org.apache.hadoop.hbase.util.RegionSizeCalculator
import org.apache.hadoop.mapreduce

class MultiSplitTableInputFormat extends TableInputFormat {

  override def getSplits(context: mapreduce.JobContext): util.List[mapreduce.InputSplit] = {
    val conf = context.getConfiguration
    val connection = ConnectionFactory.createConnection(conf)
    val tableNameStr = conf.get("table")
    val tableName = TableName.valueOf(tableNameStr)
    val table = connection.getTable(tableName)

    if (table == null) {
      throw new Exception("No table was provided.")
    }

    val scanStart = conf.get("logical.scan.start")
    val scanStop = conf.get("logical.scan.stop")
    val randChars = conf.get("logical.scan.randChars")
    val batchCount = conf.get("logical.scan.batchCount").toInt

    var startDec: Long = Long2long(java.lang.Long.valueOf(scanStart))
    var stopDec: Long = Long2long(java.lang.Long.valueOf(scanStop))

    val locator = connection.getRegionLocator(tableName)
    val splits: util.List[mapreduce.InputSplit] = new util.ArrayList[mapreduce.InputSplit](1)

    for (regionSalt <- randChars.split(',')) {
      startDec = Long2long(java.lang.Long.valueOf(scanStart))
      stopDec = Long2long(java.lang.Long.valueOf(scanStop))
      while (startDec < stopDec) {
        var temp = startDec

        if (stopDec-temp < batchCount)
          temp = temp + stopDec - temp
        else
          temp = temp + batchCount

        val startRowKey = regionSalt + "" + startDec
        val stopRowKey = regionSalt + "" + temp
        val controlStopRowKey = regionSalt + "" + (temp - 1)

        var regionLocation: String = ""

        var isSameRegion = true

        regionLocation = getTableRegionLocation(locator, startRowKey.getBytes)
        val startRowEncodedRegionName = getTableRegionName(locator, startRowKey.getBytes)
        val stopRowEncodedRegionName = getTableRegionName(locator, controlStopRowKey.getBytes)

        if (startRowEncodedRegionName.equalsIgnoreCase(stopRowEncodedRegionName)) {
          isSameRegion = true
        } else {
          isSameRegion = false
        }

        if (isSameRegion) {
          val scan: Scan = new Scan

          scan.setStartRow(startRowKey.getBytes)
          scan.setStopRow(stopRowKey.getBytes)
          scan.addFamily(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY).getBytes)
          scan.setCacheBlocks(false)

          val inputSplit = new TableSplit(table.getName, scan, scan.getStartRow, scan.getStopRow, regionLocation, startRowEncodedRegionName, 73400320L)
          splits.add(inputSplit)

        } else {
          val regionStartStopKey1 = getTableRegionStartEndKeys(locator, startRowKey.getBytes)
          val regionStartStopKey2 = getTableRegionStartEndKeys(locator, stopRowKey.getBytes)

          val scan1 = new Scan
          scan1.setStartRow(startRowKey.getBytes)
          if (startRowKey.length > regionStartStopKey1._2.length) {
            val newStop = regionStartStopKey1._2.padTo(startRowKey.length, "9").mkString
            scan1.setStopRow(newStop.getBytes)
          } else {
            scan1.setStopRow(regionStartStopKey1._2.getBytes)
          }
          scan1.setCacheBlocks(false)
          scan1.addFamily(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY).getBytes)
          regionLocation = getTableRegionLocation(locator, startRowKey.getBytes)

          val inputSplit1 = new TableSplit(table.getName, scan1, scan1.getStartRow, scan1.getStopRow, regionLocation, startRowEncodedRegionName, 73400320L)
          splits.add(inputSplit1)

          val scan2 = new Scan
          if (stopRowKey.length > regionStartStopKey2._1.length) {
            val newStart = regionStartStopKey2._1.padTo(startRowKey.length, "0").mkString
            scan2.setStartRow(newStart.getBytes)
          } else {
            scan2.setStartRow(regionStartStopKey2._1.getBytes)
          }
          scan2.setStopRow(stopRowKey.getBytes)
          scan2.setCacheBlocks(false)
          scan2.addFamily(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY).getBytes)
          regionLocation = getTableRegionLocation(locator, stopRowKey.getBytes)

          val inputSplit2 = new TableSplit(table.getName, scan2, scan2.getStartRow, scan2.getStopRow, regionLocation, stopRowEncodedRegionName, 73400320L)
          splits.add(inputSplit2)
        }

        startDec = temp
      }
    }
    splits.toArray.foreach(println(_))
    return splits
  }

  def getTableRegionLocation(regionLocator: RegionLocator, rowKey: Array[Byte]): String = {
    val regionLocation = regionLocator.getRegionLocation(rowKey).getHostname
    return regionLocation
  }

  def getTableRegionName(regionLocator: RegionLocator, rowKey: Array[Byte]): String = {
    val regionName = regionLocator.getRegionLocation(rowKey).getRegionInfo.getEncodedName
    return regionName
  }

  def getTableRegionSize(regionLocator: RegionLocator, rowKey: Array[Byte], sizeCalculator: RegionSizeCalculator): Long = {
    val regionId = regionLocator.getRegionLocation(rowKey).getRegionInfo.getRegionId
    val regionSize = sizeCalculator.getRegionSize(regionId.toString.getBytes)
    return regionSize
  }

  def getTableRegionStartEndKeys(regionLocator: RegionLocator, rowKey: Array[Byte]): (String, String) = {
    val startKey = regionLocator.getRegionLocation(rowKey).getRegionInfo.getStartKey.map(_.toChar).mkString
    val endKey = regionLocator.getRegionLocation(rowKey).getRegionInfo.getEndKey.map(_.toChar).mkString

    return (startKey, endKey)
  }

}
