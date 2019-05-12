import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    //val logFile = "/home/iman/Desktop/dataset/tweets.txt" // Should be some file on your system
//    val logFile = "/home/iman/Desktop/spark-2.4.1-bin-hadoop2.7/README.md"
val logFile = args(0) 
val conf = new SparkConf().setAppName("Simple Application")
val sc = new SparkContext(conf)
val f = sc.textFile(logFile, 2).cache()

val line = f.filter(l => l.contains("RT @") )


val rdd = line.map{ case (l) =>
  val pattern = """RT @[a-zA-Z0-9]+ """.r
  val rt = pattern.findFirstIn(l)
  (rt)}

rdd.collect().foreach(println)

val nah = rdd.map(word => (word,1)).reduceByKey(_ + _)

val wc_swap = nah.map(_.swap)

val hifreq_words = wc_swap.sortByKey(false,1)

val top10 = hifreq_words.take(11)

val top10rdd = sc.parallelize(top10)
top10rdd.saveAsTextFile("hifreq_top10")


  }
}
