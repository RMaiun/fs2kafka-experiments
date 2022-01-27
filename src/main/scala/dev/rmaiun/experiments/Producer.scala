package dev.rmaiun
package dev.rmaiun.experiments

import cats.Show
import cats.effect._
import fs2.kafka._
import fs2.{Chunk, Stream => Fs2Stream}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import cats.syntax.all._

import scala.language.postfixOps

object Producer extends IOApp {
  val log = LoggerFactory.getLogger("dev.rmaiun.dev.rmaiun.experiments.Producer")
  case class User(id: String, age: Int)
  implicit val ShowUser: Show[User] = Show.show(u => s"${u.id}|${u.age}")

  val producerSettings: ProducerSettings[IO, String, String] = ProducerSettings(
    keySerializer = Serializer[IO, String],
    valueSerializer = Serializer[IO, String]
  ).withBootstrapServers("localhost:29092")

  val stream: fs2.Stream[IO, KafkaProducer.Metrics[IO, String, String]] = KafkaProducer.stream(producerSettings)

  val consumerSettings =
    ConsumerSettings[IO, String, String](
      keyDeserializer =
        Deserializer.instance[IO, String]((k, h, b) => Option(b).fold(IO("null"))(x => IO(new String(x)))),
      valueDeserializer =
        Deserializer.instance[IO, String]((k, h, b) => Option(b).fold(IO("null"))(x => IO(new String(x))))
    )
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:29092")
      .withGroupId("G-1")

  def run(args: List[String]): IO[ExitCode] = {
//    val stream =
//      KafkaConsumer
//        .stream(consumerSettings)
//        .subscribeTo("t2")
//        .records
//        .map { committable =>
//          val key   = committable.record.key
//          val value = committable.record.value
//          println(s"-------------key=$key, value=$value")
//          val record = ProducerRecord("t2", UUID.randomUUID().toString, value)
//          ProducerRecords.one(record, committable.offset)
//        }
//        .through(KafkaProducer.pipe(producerSettings))
//
//    stream.compile.drain.as(ExitCode.Success)

    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo("t2")
      .records
      .evalTap { committable =>
        val key            = committable.record.key
        val value          = committable.record.value
        val topicPartition = committable.offset.topicPartition
        val offset         = committable.offset.offsetAndMetadata.offset()
        IO.delay(log.info(s"key=$key|value=$value|topicPartition=$topicPartition|offset=$offset"))
      }.concurrently(KafkaProducer
      .stream(producerSettings)
      .flatMap(producer => doRepeat(producer)))
      .compile
      .drain
      .as(ExitCode.Success)

  }

  def doRepeat(producer: KafkaProducer.Metrics[IO, String, String]): Fs2Stream[IO, Chunk[ProducerResult[Unit, String, String]]] = {
    val x = new AtomicInteger()
    Fs2Stream
      .repeatEval(IO(s"${UUID.randomUUID().toString}"))
      .take(1_000_000)
      .map(str => ProducerRecord("t2", String.valueOf(x.getAndIncrement()), str))
      .evalMap(record => producer.produce(ProducerRecords.one(record)))
      .groupWithin(50, 10 seconds)
      .evalMap(_.sequence)
  }
}
