package dev.rmaiun.experiments.reactor;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

public class ExactlyOnceReceiver {

  private static final Logger log = LoggerFactory.getLogger(ExactlyOnceReceiver.class);

  private static final String BOOTSTRAP_SERVERS = "localhost:29092";
  private static final String TOPIC = "t2";

  public static void main(String[] args) throws InterruptedException {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, CustomStringDeser.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    ReceiverOptions<String, String> receiverOptions = ReceiverOptions.create(props);
    var dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    ReceiverOptions<String, String> options = receiverOptions.subscription(Collections.singleton(TOPIC))
        .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
        .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));

    var count = 20;
    var latch = new CountDownLatch(count);

    Flux<ReceiverRecord<String, String>> kafkaFlux = KafkaReceiver.create(options).receive();
    var disposable = kafkaFlux
        .subscribe(record -> {
      ReceiverOffset offset = record.receiverOffset();
      System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%s value=%s\n",
          offset.topicPartition(),
          offset.offset(),
          dateFormat.format(new Date(record.timestamp())),
          record.key(),
          record.value());
      offset.acknowledge();
      latch.countDown();
    });
    latch.await(10, TimeUnit.SECONDS);
    disposable.dispose();
  }
}
