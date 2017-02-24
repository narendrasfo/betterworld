package examples


import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object AverageFromFiles {

 def main(args: Array[String]) {
   Logger.getLogger("org").setLevel(Level.OFF);
   Logger.getLogger("akka").setLevel(Level.OFF);
   startProccess
  }

  def startProccess(): Unit ={
   val conf = new SparkConf().setAppName("avg").setMaster("local[2]")
    val sc = new  SparkContext(conf)
    println(average(sc))
 }

  import scala.collection.JavaConversions._
    // TODO optimize average
   def average(sc: SparkContext):Double = {
      val rdd = sc.textFile("src/test/data/numbers.txt").map(x => x.split("\\,")).flatMap(x => x).filter(x => (x.toInt % 2 == 0)).map(x => x.toInt)

      def countSum (iter: Iterator[Int]) : Iterator[(Int,Int)] = {
        val arr = new util.ArrayList[(Int, Int)]()
        var sumVal = 0;
        var cnt = 0;
        while (iter.hasNext) {
          val st = iter.next()
          sumVal = sumVal + st
          cnt = cnt + 1
        }

        arr.add((sumVal, cnt))
        sumVal = 0;
        cnt = 0;
        arr.iterator()
      }

      
      val rd = rdd.mapPartitions(x => countSum(x) )

      rd.foreach(println)
      println("sum " + rdd.sum)
      println("count " + rdd.count)
      val arr =  rd.collect()
      var sum = 0
      var ct = 0
      arr.foreach(x => {sum = sum + x._1 ; ct = ct + x._2 })

      println(sum,ct,sum/ct)
      rdd.sum / rdd.count()
    }

}
