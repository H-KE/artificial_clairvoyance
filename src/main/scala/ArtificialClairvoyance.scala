import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Top level application container.
 * This will set up the necessary context and invoke our application
 * TODO: this is just a stub right now
 **/
object ArtificialClairvoyance {
  def main(args: Array[String]) {
    // Should be some file on your system
    val logFile = "/Users/yk_yosuke_mori/Documents/spark/artificial-clairvoyance/README.md"
    val conf = new SparkConf().setAppName("Artificial Clairvoyance")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
