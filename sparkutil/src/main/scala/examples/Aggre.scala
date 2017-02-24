package examples

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import util.DataFrameBuilder


object Aggre {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("avg").setMaster("local[2]")
    val sc = new  SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val rdd =sc.textFile("src/test/data/numbers.txt").map(x => Row(x.toString))
    DataFrameBuilder.buildDF(Seq("col"),Seq("String"),rdd,sQLContext).show()


  }
}
