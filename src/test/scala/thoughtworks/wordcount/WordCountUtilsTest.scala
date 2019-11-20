package thoughtworks.wordcount

import thoughtworks.DefaultFeatureSpecWithSpark


class WordCountUtilsTest extends DefaultFeatureSpecWithSpark {
  feature("Split Words") {
    scenario("test splitting a dataset of words by spaces") {
      import spark.implicits._
      val expectedResult = Seq("worst", "times").toDS().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst times").toDS()).splitWords(spark).collect()

      actualResult.shouldEqual(expectedResult)
    }

    scenario("test splitting a dataset of words by period") {
      import spark.implicits._
      val expectedResult = Seq("worst", "times").toDS().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst. times").toDS()).splitWords(spark).collect()

      actualResult.shouldEqual(expectedResult)
    }

    scenario("test splitting a dataset of words by comma") {
      import spark.implicits._
      val expectedResult = Seq("worst", "times").toDS().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst, times").toDS()).splitWords(spark).collect()

      actualResult.shouldEqual(expectedResult)
    }

    scenario("test splitting a dataset of words by hypen") {
      import spark.implicits._
      val expectedResult = Seq("worst", "times").toDS().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst--times").toDS()).splitWords(spark).collect()

      actualResult.shouldEqual(expectedResult)
    }

    scenario("test splitting a dataset of words by semi-colon") {
      import spark.implicits._
      val expectedResult = Seq("worst", "times").toDS().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst; times").toDS()).splitWords(spark).collect()

      actualResult.shouldEqual(expectedResult)
    }
  }

  feature("Count Words") {
    ignore("basic test case") {}

    ignore("should not aggregate dissimilar words") {}

    ignore("test case insensitivity") {}
  }

  feature("Sort Words") {
    ignore("test ordering words") {}
  }

}
