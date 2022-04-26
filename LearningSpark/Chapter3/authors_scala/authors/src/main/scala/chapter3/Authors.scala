package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object Authors {
  val session = SparkSession.builder().appName("AuthorsAge").getOrCreate()

  val dataDf = session.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25), ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

  val avgDf = dataDf.groupBy("name").agg(avg("age"))
  avgDf.show()
}