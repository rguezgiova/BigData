package chapter3

import org.apache.spark.sql.types._

object Schema {
  val schema = StructType(Array(StructField("author", StringType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("pages", IntegerType, nullable = false)))

  val schema2 = "author STRING, title STRING, pages INT"
}