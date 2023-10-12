import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
object ReadCSVFile {
   def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val configuration = new SparkConf()
      .setAppName("Read CSV File")
      .setMaster("local")
    val sparkContext = new SparkContext(configuration)

    val textFilePath = "src/main/resources/elonmusk_tweets.csv"
    val rddText = sparkContext.textFile(textFilePath).filter(!_.startsWith("id"))
    //** println(rddText.foreach(println))

    //1. the distribution of keywords over time (day-wise), i.e., the number of times each keyword is mentioned every day:
    val keywordsInput = scala.io.StdIn.readLine("Please enter comma-separated list of keywords : ")
    val keywordsList = keywordsInput.split(",").map(_.trim).toList

    val rddKeywords = rddText.filter(line =>
      keywordsList.exists(keyword => line.toUpperCase.contains(keyword.toUpperCase))
    )
    val distributionKeywordsByDateAndNumberTimesMentioned = rddKeywords.map(line => line.split(",")(1).substring(0, 10))
                                                             .map(date => (date, 1)).reduceByKey(_ + _)

    println("1. The distribution of keywords over time (day-wise), i.e., the number of times each keyword is mentioned every day like this (k1, 12-7-2013,45)")
    keywordsList.foreach { currentKeyword =>
      val keywordResults = distributionKeywordsByDateAndNumberTimesMentioned.collect().map { case (date, count) =>
        s"$currentKeyword,$date,$count"
      }
      keywordResults.foreach(println)
    }
    //2. the percentage of tweets that have at least one of these input keywords.
    val totalTweets = rddText.count()
    //println(totalTweets)
    val numberKeyword = rddKeywords.count()
    // println(numberKeyword)
    val percentage = (numberKeyword.toFloat / totalTweets.toFloat) * 100.00
    print(s"2. The percentage of tweets that have at least one of these input keywords :  $percentage ")

    // 3. the percentage of tweets that have exactly two input keywords.
    val keywordRddExactlyTwoWord = rddText.filter(line =>
      keywordsList.count(keyword => line.toUpperCase.contains(keyword.toUpperCase)) == 2
    )
    val numberExactlyTwoKeyword = keywordRddExactlyTwoWord.count()
    // println(numberExactlyTwoKeyword)
    val percentageExactlyTwoKeyword = (numberExactlyTwoKeyword.toFloat / totalTweets.toFloat) * 100.00
    print(s"3. The percentage of tweets that have exactly two input keywords :  $percentageExactlyTwoKeyword")

    //4.1. the average of the length of tweets.
    val tweetsLength = rddKeywords.map(line => {
      val words = line.split(",")(2).split("\\s+")
       (line.split(",")(2), words.length)
    })
     println(tweetsLength.foreach(println))

     val sumAllValues = if (tweetsLength.isEmpty) {
       throw new RuntimeException("The tweets length is Empty collection ")
     } else {
       tweetsLength.map(_._2).reduce(_ + _)
     }
     //println(sumAllValues)
    var averageOfTweets = 0.0
     if (numberKeyword != 0) {
        averageOfTweets = sumAllValues.toFloat / numberKeyword.toFloat
       println(s"4.1. The average of the length of tweets :   $averageOfTweets")
     } else {
       throw new ArithmeticException("Division by zero: the Number of Keyword is zero.")
     }
    // 4.2. the standard deviation of the length of tweets .
    val lengthAllValues = rddKeywords.map(line => (line.split(",")(2), (line.split(",")(2).length))).map(_._2)
   // println(lengthAllValues.foreach(println))
    val squareSubtractionOfValues = lengthAllValues.map(value=>(math.pow(value - averageOfTweets,2).toFloat))
   // println(squareSubtractionOfValues.foreach(println))
    val variance = squareSubtractionOfValues.sum().toFloat / numberKeyword.toFloat
    //println(variance)
    val standardDeviationOfTweets = math.sqrt(variance).toFloat
    print(s"4.2. the standard deviation of the length of tweets : $standardDeviationOfTweets ")
  }

}

