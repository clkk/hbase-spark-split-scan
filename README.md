# hbase-spark-split-scan
When the data will be scan between two rowKeys, will be split by region count in the region servers. If we have more executors than region count, we can't use the all executors for scanning. In order to use the executors at maximum efficiency during the scan, the scan operation is divided by the specified batch count value. We can take advantage of all Executors on this splitted scan. Also, in this sample is simulated scan operation for the salting char enabled HBase table.

