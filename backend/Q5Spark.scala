import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.util.parsing.json._

/**
 * Created by Shimin Wang on 11/27/15.
 */
class Q5Spark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Q5Spark")
    val sc = new SparkContext(conf)

    //        val logFile = "s3://shiminwang/ccteamQ5/smallAllData.txt"
    val logFile = "s3://cmucc-datasets/twitter/f15/"
    val textFile = sc.textFile(logFile)
    val result = textFile.map{line => val jsonObj = JSON.parseFull(line).get.asInstanceOf[Map[String, Any]]
      val tid = jsonObj.get("id_str").get.asInstanceOf[String]
      val userobj = jsonObj.get("user").asInstanceOf[Some[Map[String, Any]]].get
      val uid = userobj.get("id_str").get.asInstanceOf[String]
      (uid.toLong, tid)
    }.groupByKey().mapValues(tidlist => tidlist.toArray.distinct.size).map{pair => pair._1 + "\t" + pair._2}.saveAsTextFile("s3://shiminwang/ccteamQ5/output/")

  }
}
