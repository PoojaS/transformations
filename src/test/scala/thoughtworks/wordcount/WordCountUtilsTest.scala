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
    scenario("basic test case") {
      import spark.implicits._
      val expectedResult = Seq(("worst", BigInt(2))).toDF().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst", "worst").toDS()).countByWord(spark).collect()

      actualResult.contains(expectedResult)
      expectedResult.contains(actualResult)
    }

    scenario("should not aggregate dissimilar words") {
      import spark.implicits._
      val expectedResult = Seq(("times", BigInt(1)), ("worst", BigInt(2))).toDF().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst", "times", "worst").toDS()).countByWord(spark).collect()

      actualResult.contains(expectedResult)
      expectedResult.contains(actualResult)
    }

    scenario("test case insensitivity") {
      import spark.implicits._
      val expectedResult = Seq(("times", BigInt(1)), ("worst", BigInt(2))).toDF().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("Worst", "Times", "worst").toDS()).countByWord(spark).collect()

      actualResult.contains(expectedResult)
      expectedResult.contains(actualResult)
    }
  }

  feature("Sort Words") {
    scenario("test ordering words") {
      import spark.implicits._
      val expectedResult = Seq("banana", "times", "worst", "worst").toDF().collect()

      val actualResult = WordCountUtils.StringDataset(Seq("worst", "times", "worst", "banana").toDS()).sortWords(spark).collect()

      actualResult.contains(expectedResult)
      expectedResult.contains(actualResult)
    }
  }

}
