package dev.rmaiun
package dev.rmaiun.experiments.fs2

import cats.Show
import cats.effect._
import fs2.kafka._
import fs2.{ Stream => Fs2Stream }
import org.slf4j.{ Logger, LoggerFactory }

import java.util.Random
import scala.concurrent.duration._

object AsyncConsumer extends IOApp {
  val log: Logger = LoggerFactory.getLogger("dev.rmaiun.experiments.AsyncConsumer")
  case class User(id: String, age: Int)
  implicit val ShowUser: Show[User] = Show.show(u => s"${u.id}|${u.age}")

  val consumerSettings: ConsumerSettings[IO, String, String] =
    ConsumerSettings[IO, String, String](
      keyDeserializer =
        Deserializer.instance[IO, String]((k, h, b) => Option(b).fold(IO("null"))(x => IO(new String(x)))),
      valueDeserializer =
        Deserializer.instance[IO, String]((k, h, b) => Option(b).fold(IO("null"))(x => IO(new String(x))))
    )
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:29092")
      .withGroupId("G-1")

  def run(args: List[String]): IO[ExitCode] =
    consumer.compile.drain
      .as(ExitCode.Success)

  def consumer: Fs2Stream[IO, Unit] = KafkaConsumer
    .stream(consumerSettings)
    .subscribeTo("t2")
    .records
    .mapAsync(4) { committable =>
      val key    = committable.record.key
      val value  = committable.record.value
      val offset = committable.offset.offsetAndMetadata.offset()
      val random = new Random().nextInt(10)
      if (value == "7") {
        Clock[IO].sleep(random.seconds) *>
          IO.delay(log.info(s"value=$value|offset=$offset|DELAYED=$random"))
            .as(committable.offset)
      } else {
        IO.raiseError(new RuntimeException("Some error"))
      }

    }
    .through(commitBatchWithin(4, 15.seconds)(implicitly[Temporal[IO]]))
}
