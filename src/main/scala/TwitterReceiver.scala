import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._

import java.io.{OutputStream, PrintStream}
import scala.concurrent.{Future, Promise}

class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY) {

	import scala.concurrent.ExecutionContext.Implicits.global

	/**
	 * [TwitterStream] => Wrapper over TWITTER-REST-API<br />
	 * [twitterStreamFuture] => on onStop => clean up twitter streams<br />
	 *
	 * [nb:]
	 * We’ve created a new Promise which will eventually be given a value.<br />
	 * We then call a library that uses completion listeners, and in its completion, we fill in the Promise.<br />
	 * Finally, we spin off a Future which gives us a read-only view into the Promise<br />
	 *
	 * - onStart & onStop => run asynchronously<br />
	 */
	val twitterStreamPromise: Promise[TwitterStream] = Promise[TwitterStream]()
	val twitterStreamFuture: Future[TwitterStream] = twitterStreamPromise.future

	def simpleStatusListener: StreamListener = new StatusListener {

		/// onStatus => receive new tweet => store() => store tweet for spark

		override def onStatus(status: Status): Unit = store(status)

		override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()

		override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()

		override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()

		override def onStallWarning(warning: StallWarning): Unit = ()

		override def onException(ex: Exception): Unit = ex.printStackTrace()
	}

	/**
	 * [] called by system error println => standford nlp use:
	 */
	private def redirectSystemError(): Unit = System.setErr(new PrintStream(new OutputStream {
		override def write(b: Array[Byte]): Unit = ()

		override def write(b: Array[Byte], off: Int, len: Int): Unit = ()

		override def write(b: Int): Unit = ()

	}))

	override def onStart(): Unit = {
		redirectSystemError()
		val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
			.getInstance()
			.addListener(simpleStatusListener)
			.sample("en")

		twitterStreamPromise.success(twitterStream)
	}

	override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
		twitterStream.cleanUp()
		twitterStream.shutdown()
	}
}
