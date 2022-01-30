package dev.rmaiun
package dev.rmaiun.experiments

import cats.Show
import cats.effect._
import cats.syntax.all._
import fs2.kafka._
import fs2.{Chunk, Stream => Fs2Stream}
import org.slf4j.{Logger, LoggerFactory}

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.language.postfixOps

object ProducerAnd2Consumers extends IOApp {
  val log: Logger = LoggerFactory.getLogger("dev.rmaiun.experiments.ProducerAnd2Consumers")
  case class User(id: String, age: Int)
  implicit val ShowUser: Show[User] = Show.show(u => s"${u.id}|${u.age}")

  val noth: Array[Byte] = null

  val producerSettings: ProducerSettings[IO, String, String] = ProducerSettings(
    keySerializer = Serializer[IO, String],
    valueSerializer = Serializer[IO, String]
  )
    .withBootstrapServers("localhost:29092")

  val stream: fs2.Stream[IO, KafkaProducer.Metrics[IO, String, String]] = KafkaProducer.stream(producerSettings)

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
    consumer("1")
      .concurrently(consumer("2"))
      .concurrently(
        KafkaProducer
          .stream(producerSettings)
          .flatMap(producer => doRepeat(producer))
      )
      .compile
      .drain
      .as(ExitCode.Success)

  def consumer(n: String): Fs2Stream[IO, Unit] = KafkaConsumer
    .stream(consumerSettings)
    .subscribeTo("t2")
    .records
    .evalMap { committable =>
      val key            = committable.record.key
      val value          = committable.record.value
      val topicPartition = committable.offset.topicPartition
      val offset         = committable.offset.offsetAndMetadata.offset()
      IO.delay(log.info(s"#$n|key=$key|offset=$offset|topicPartition=$topicPartition|value=$value"))
        .as(committable.offset)
    }
    .through(commitBatchWithin(1000, 15.seconds)(implicitly[Temporal[IO]]))

  def doRepeat(
    producer: KafkaProducer.Metrics[IO, String, String]
  ): Fs2Stream[IO, Chunk[ProducerResult[Unit, String, String]]] = {
    val x = new AtomicInteger()
    Fs2Stream
      .repeatEval(IO(s"${UUID.randomUUID().toString}"))
      .take(0)
      .map(str => ProducerRecord("t2", String.valueOf(x.incrementAndGet()), str))
      .evalMap(record => producer.produce(ProducerRecords.one(record)))
      .groupWithin(1000, 10 seconds)
      .evalMap(_.sequence)
  }
}
