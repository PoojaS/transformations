package thoughtworks.wordcount

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object WordCountUtils {

  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {

      import spark.implicits._
      val splitDataSet = dataSet.flatMap(line => line.replaceAll("\"|,|\\.|;", "")
        .replace("--", " ")
        .toLowerCase()
        .split(" "))
      splitDataSet

    }


    def countByWord(spark: SparkSession) = {
      dataSet.sort("value").groupBy("value").count()
    }
  }

}
