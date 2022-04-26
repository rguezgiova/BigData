from pyspark.sql.types import *

schema = StructType([StructField("author", StringType(), False),
                     StructField("title", StringType(), False),
                     StructField("pages", IntegerType(), False)])

schema2 = "author STRING, title String, pages INT"