package dev.rmaiun.experiments.reactor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class ExactlyOnceSender {

  private static final Logger log = LoggerFactory.getLogger(ExactlyOnceSender.class);

  private static final String BOOTSTRAP_SERVERS = "localhost:29092";
  private static final String TOPIC = "t2";

  public static void main(String[] args) throws InterruptedException {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);

    var dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");
    var sender = KafkaSender.create(senderOptions);
    var count = 20;
    var latch = new CountDownLatch(count);
    sender.<Integer>send(Flux.range(1, count)
            .map(i -> SenderRecord.create(new ProducerRecord<>(TOPIC, i, "Message_" + i), i)))
        .doOnError(e -> log.error("Send failed", e))
        .subscribe(r -> {
          RecordMetadata metadata = r.recordMetadata();
          System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
              r.correlationMetadata(),
              metadata.topic(),
              metadata.partition(),
              metadata.offset(),
              dateFormat.format(new Date(metadata.timestamp())));
          latch.countDown();
        });
    latch.await(10, TimeUnit.SECONDS);
    sender.close();


  }

}
