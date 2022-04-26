package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object blogs {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("Example-3_7").getOrCreate()

    if (args.length <= 0) {
      print("Usage: blogs <file>")
      System.exit(1)
    }

    val file = args(0)

    val schema = StructType(Array(StructField("Id", IntegerType, nullable = false),
      StructField("First", StringType, nullable = false),
      StructField("Last", StringType, nullable = false),
      StructField("Url", StringType, nullable = false),
      StructField("Published", StringType, nullable = false),
      StructField("Hits", IntegerType, nullable = false),
      StructField("Campaigns", ArrayType[String], nullable = false)))

    val blogsDf = session.read.schema(schema).json(file)
    blogsDf.show(false)
    println(blogsDf.printSchema())
    println(blogsDf.schema)
  }
}