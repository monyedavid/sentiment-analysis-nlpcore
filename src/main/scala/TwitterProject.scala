import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterProject {
	val spark: SparkSession = SparkSession
		.builder()
		.appName("Twitter Project")
		.master("local[*]")
		.getOrCreate()

	spark.sparkContext.setLogLevel("WARN")

	val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

	def readTwitter(): Unit = {
		val twitterStreamTweets: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
		val tweets: DStream[String] = twitterStreamTweets.map { tweet_status =>
			val username = tweet_status.getUser.getName
			val followers = tweet_status.getUser.getFollowersCount
			val text = tweet_status.getText
			s"User $username ($followers followers) says: $text"
		}

		tweets.print()
		ssc.start()
		ssc.awaitTermination()
	}

	def getAverageTweetLength: DStream[Double] = {
		val tweets: DStream[Status] = ssc.receiverStream(new TwitterReceiver)

		tweets
			.map(_.getText)
			.map(text => text.length)
			.map(len => (len, 1)) // => (TweetLength, Count)
			.reduceByWindow(
				(tuple1, tuple2) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2),
				Seconds(5),
				Seconds(5))
			.map { megaTuple =>
				val tweetLengthSum = megaTuple._1
				val tweetCount = megaTuple._2

				tweetLengthSum * 1.0 / tweetCount
			}
	}

	def computeMostPopularHashtags: DStream[(String, Int)] = {
		val tweets: DStream[Status] = ssc.receiverStream(new TwitterReceiver)

		ssc.checkpoint("checkpoints")

		tweets
			.map(_.getHashtagEntities) // -> DStream[HashTagEntities]
			.flatMap(hashTagEntities => hashTagEntities.map(_.getText))
			.map((_, 1))
			.reduceByKeyAndWindow((x, y) => x + y, _ - _, Seconds(60), Seconds(10))
			.transform(_.sortBy(tuple => -tuple._2))
	}

	def readTwitterWithSentiments(): Unit = {
		val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
		val tweets: DStream[String] = twitterStream.map { status =>
			val username = status.getUser.getName
			val followers = status.getUser.getFollowersCount
			val text = status.getText
			val sentiment = SentimentAnalysis.detectSentiment(text)

			s"User $username ($followers followers) says $sentiment: $text"
		}

		tweets.print()
		ssc.start()
		ssc.awaitTermination()
	}

	def main(args: Array[String]): Unit = {
		readTwitterWithSentiments()
	}

}
