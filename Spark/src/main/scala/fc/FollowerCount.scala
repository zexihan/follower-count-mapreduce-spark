package fc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object FollowerCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.FollowerCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Follower Count")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))
    val counts = textFile.map(line => line.split(","))
                 .map(user => (user(1), 1))
                 .reduceByKey(_ + _)
    
    // Handle users with 0 follower. This method works but it seems to double the load, which is not ideal.
//    def g(user: Array[String]) = Array((user(0), 0), (user(1), 1))
//    val counts = textFile.map(line => line.split(","))
//                 .flatMap(user => g(user))
//                 .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
  }
}