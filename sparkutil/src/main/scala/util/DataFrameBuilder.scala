package util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.types._


object DataFrameBuilder {


  def buildDF(column: Seq[String], dataType:Seq[String], rdd:RDD[Row], sqlContext:SQLContext ): DataFrame = {
    val datatypes = dataType.map(x => x.toLowerCase).map {
      case "String" => StringType
      case "IntegerType" => IntegerType
      case "LongType" => LongType
      case "DoubleType" => DoubleType
      case "FloatType" => FloatType
      case "ByteType" => ByteType
      case "ShortType" => ShortType
      case "DateType" => DateType
      case "BooleanType" => BooleanType
      case "TimestampType" => TimestampType
      case _ => StringType
    }

    val str = StructType((0 to column.size - 1).map(x =>
      StructField(column(x), datatypes(x), true)).toArray)
    str.printTreeString()

    sqlContext.createDataFrame(rdd,str)
  }

}
