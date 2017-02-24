package examples

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import util.DataFrameBuilder
import org.apache.spark.sql.functions._

object AggrDF {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("avg").setMaster("local[2]")
    val sc = new  SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // dataframe
    val rdd =sc.textFile("src/test/data/numbers.txt").map(x =>x.split(",")).flatMap(x=>x).map(x => Row(x.toInt))
    DataFrameBuilder.buildDF(Seq("col"),Seq("integerType"),rdd,sQLContext).select(avg(col("col")),sum(col("col")),count(col("col"))).show()
    // using RDD


  }
}
