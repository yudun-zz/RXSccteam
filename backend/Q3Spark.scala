import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by tengjiaochen on 11/30/15.
 */
class Q3Spark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Q3Spark")
    val sc = new SparkContext(conf)

    val logFile = "s3://ruiruix4600/phase3_q3_out/*"
    val textFile = sc.textFile(logFile)
    val result = textFile.map{line => val item = line.split("\\t")
        val group = item(0).toLong % 8
      (group, line)
    }.groupByKey().cache()

      for(i <- 0 to 7) {
        result.filter(_._1 == i).values.flatMap(_.toList).saveAsTextFile("s3://rxs-phase3/q3data/group" + i)
      }

  }
}
