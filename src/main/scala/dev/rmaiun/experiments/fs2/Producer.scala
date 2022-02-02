package dev.rmaiun
package dev.rmaiun.experiments.fs2

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Chunk
import fs2.kafka._
import fs2.{Stream => Fs2Stream}

import scala.concurrent.duration._
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import cats.syntax.all._

import scala.language.postfixOps

object Producer extends IOApp {

  val producerSettings: ProducerSettings[IO, String, String] =
    ProducerSettings(keySerializer = Serializer[IO, String], valueSerializer = Serializer[IO, String])
      .withBootstrapServers("localhost:29092")

  override def run(args: List[String]): IO[ExitCode] =
    KafkaProducer
      .stream(producerSettings)
      .flatMap(producer => doRepeat(producer))
      .compile
      .drain
      .as(ExitCode.Success)

  def doRepeat(
    producer: KafkaProducer.Metrics[IO, String, String]
  ): Fs2Stream[IO, Chunk[ProducerResult[Unit, String, String]]] = {
    val x = new AtomicInteger()
    Fs2Stream
      .repeatEval(IO(s"${UUID.randomUUID().toString}"))
      .take(16)
      .map { _ =>
        val k = x.incrementAndGet()
        println(k)
        ProducerRecord("t2", "1", k.toString)
      }
      .evalMap(record => producer.produce(ProducerRecords.one(record)))
      .groupWithin(5, 1 seconds)
      .evalMap(_.sequence)
  }
}
