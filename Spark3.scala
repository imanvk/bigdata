import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    //val logFile = "/home/iman/Desktop/dataset/tweets.txt" // Should be some file on your system
//    val logFile = "/home/iman/Desktop/spark-2.4.1-bin-hadoop2.7/README.md"
val logFile = args(0)
val cityFile =args(1)

val conf = new SparkConf().setAppName("Simple Application")
val sc = new SparkContext(conf)
val f = sc.textFile(logFile, 2).cache()


val f1 = sc.textFile(cityFile, 2).cache()


val lineC = f1.filter(l => l.contains("Los Angeles"))



val rdd = lineC.map{ case (l) =>
  val pattern = """^\d+""".r
  val rt = pattern.findFirstIn(l)
  (rt)}


val laUsers = rdd.collect.toSet

val rdd1 = f.map{ case (l) =>
  val pattern1 = """^\d+""".r
  val rt1 = pattern1.findFirstIn(l)
  (rt1)}


val rdd2 = rdd1.filter{ case (l) => laUsers.contains(l)}


rdd2.collect().foreach(println)


val nah = rdd2.map(word => (word,1)).reduceByKey(_ + _)

val wc_swap = nah.map(_.swap)

val hifreq_words = wc_swap.sortByKey(false,1)

val top10 = hifreq_words.take(11)

val top10rdd = sc.parallelize(top10)
top10rdd.saveAsTextFile("hifreq_top10")


  }
}
