package examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row

object AverageFromFiles {

 def main(args: Array[String]) {
   startProccess
  }

  def startProccess(): Unit ={
   val conf = new SparkConf().setAppName("avg").setMaster("local[2]")
    val sc = new  SparkContext(conf)
    println(average(sc))
 }


    // TODO optimize average
   def average(sc: SparkContext):Double ={
     val rdd =sc.textFile("src/test/data/numbers.txt").map(x => x.split("\\,")).flatMap(x =>x).filter(x => (x.toInt%2==0)).map(x=> x.toInt)
     rdd.sum/rdd.count()
   }

}
