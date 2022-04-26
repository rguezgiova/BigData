from pyspark.sql import SparkSession

schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STING, `Published` STRING, `Hits` INT, `Campaigns` " \
         "ARRAY<STRING> "

data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter, LinkedIn"]],
        [2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8988, ["twitter, LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter, FB, LinkedIn"]],
        [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter, FB"]],
        [5, "Matei", "Zaharia", "https://tinyurl.5", "5/4/2014", 40578, ["web", "twitter, FB, LinkedIn"]],
        [6, "Reynolds", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
        ]

if __name__ == "__main__":
        session = (SparkSession.builder.appName("Example-3_6").getOrCreate())

        blogs_df = session.createDataFrame(data, schema)
        blogs_df.show()
        print(blogs_df.printSchema())